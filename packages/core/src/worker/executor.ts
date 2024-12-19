import { InvalidStateError } from '../errors.js';
import { type ExecutorBackend } from '../types/backend.js';
import {
  JobState,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobError,
  type JobFactory,
  type JobId,
  type JobRecord,
  type JobResult,
} from '../types/job.js';
import { type QueueEventEmitter } from '../types/queue.js';
import { type RunningWorker } from '../types/worker.js';

export class Executor<BaseJob extends Job<JobArgs>> {
  private _jobRecord: JobRecord<InferJobArgs<BaseJob>>;

  private _job?: BaseJob;
  private _worker: RunningWorker<BaseJob>;
  private abortController: AbortController = new AbortController();

  constructor(
    jobRecord: JobRecord<InferJobArgs<BaseJob>>,
    worker: RunningWorker<BaseJob>,
    private backend: ExecutorBackend,
    private jobFactory: JobFactory<BaseJob>,
    private notifier: QueueEventEmitter<BaseJob>,
  ) {
    this._jobRecord = { ...jobRecord };
    this._worker = worker;
  }

  get job(): BaseJob {
    if (!this._job) {
      this._job = this.jobFactory.createJobObject(this);
    }
    return this._job;
  }

  get jobRecord(): Readonly<JobRecord<InferJobArgs<BaseJob>>> {
    return { ...this._jobRecord };
  }

  get id(): JobId {
    return this._jobRecord.id;
  }

  get taskName(): string {
    return this._jobRecord.taskName;
  }

  get args(): Readonly<InferJobArgs<BaseJob>> {
    return this._jobRecord.args;
  }

  get result(): Readonly<JobResult> | undefined {
    return this._jobRecord.result;
  }

  get state(): JobState {
    return this._jobRecord.state;
  }

  get progress(): number {
    return this._jobRecord.progress;
  }

  get maxAttempts(): number {
    return this._jobRecord.maxAttempts;
  }

  get attempt(): number {
    return this._jobRecord.attempt;
  }

  get worker(): RunningWorker<BaseJob> {
    return this._worker;
  }

  get metadata(): Readonly<Record<string, any>> {
    return this._jobRecord.metadata;
  }

  get startedAt(): Date | undefined {
    return this._jobRecord.startedAt;
  }

  get finishedAt(): Date | undefined {
    return this._jobRecord.finishedAt;
  }

  get duration(): number {
    if (this._jobRecord.startedAt) {
      if (this._jobRecord.finishedAt) {
        return this._jobRecord.finishedAt.getTime() - this._jobRecord.startedAt.getTime();
      } else {
        return Date.now() - this._jobRecord.startedAt.getTime();
      }
    }
    return 0;
  }

  get abortSignal(): AbortSignal {
    return this.abortController.signal;
  }

  async updateProgress(progress: number): Promise<boolean> {
    const isUpdated = await this.backend.updateJobProgress(this.id, this.attempt, progress);
    if (isUpdated) {
      this._jobRecord.progress = progress;

      if (this.notifier.listenerCount('job_progress') > 0) {
        const event = {
          jobRecord: this.jobRecord,
          progress,
          duration: this.duration,
          job: this.job,
        };
        this.notifier.emit('job_progress', event);
      }

      await this._worker.heartbeat();
    }
    return isUpdated;
  }

  async amendMetadata(records: Record<string, any>): Promise<boolean> {
    const metadata = await this.backend.amendJobMetadata(this.id, this.attempt, records);
    const isUpdated = metadata !== undefined;
    if (isUpdated) {
      this._jobRecord.metadata = metadata;
    }
    return isUpdated;
  }

  /**
   * Perform job and wait for it to finish.
   */
  async perform(throwOnError: boolean = false): Promise<void> {
    if (![JobState.Pending, JobState.Scheduled, JobState.Running].includes(this.state)) {
      throw new InvalidStateError(`Try to perform job with state ${this.state}: ${this.id}`);
    }
    const worker = this._worker;

    const abortEventHandler = (event: Event) => {
      if (event.type === 'abort') {
        this.abortController.abort(this.abortSignal.reason);
      }
    };

    this._jobRecord.state = JobState.Running;
    this._jobRecord.startedAt = new Date();
    this._jobRecord.finishedAt = undefined;

    if (this.notifier.listenerCount('job_started') > 0) {
      const event = {
        jobRecord: this.jobRecord,
        job: this.job,
      };
      this.notifier.emit('job_started', event);
    }

    try {
      this.abortSignal.throwIfAborted();
      this.abortSignal.addEventListener('abort', abortEventHandler);

      const task = worker.getTask(this.taskName);
      const result = await task.handle(this.job);
      await this.markSucceeded(result ?? {});
    } catch (error: any) {
      await this.markFailed(error);
      if (throwOnError) throw error;
    } finally {
      this.abortSignal.removeEventListener('abort', abortEventHandler);

      if (this.notifier.listenerCount('job_finished') > 0) {
        const event = {
          jobRecord: this.jobRecord,
          state: this.state,
          duration: this.duration,
          job: this.job,
        };
        this.notifier.emit('job_finished', event);
      }
    }
  }

  /**
   * Transition from `running` to `succeeded` state with or without a result.
   */
  async markSucceeded(result?: JobResult): Promise<boolean> {
    const isUpdated = await this.backend.markJobFinished(this.id, this.attempt, JobState.Succeeded, result ?? {});
    if (isUpdated) {
      this._jobRecord.result = result;
      this._jobRecord.state = JobState.Succeeded;
      this._jobRecord.progress = 1.0;
      this._jobRecord.finishedAt = new Date();
      if (this.notifier.listenerCount('job_succeeded') > 0) {
        const event = {
          jobRecord: this.jobRecord,
          result: { ...result },
          duration: this.duration,
          job: this.job,
        };
        this.notifier.emit('job_succeeded', event);
      }
    }
    return isUpdated;
  }

  /**
   * Transition from `running` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `pending` with a delay based on the backoff policy.
   */
  async markFailed(result: JobError = new Error('Unknown error')): Promise<boolean> {
    if (result instanceof Error) {
      result = { name: result.name, message: result.message, stack: result.stack };
    }
    const isUpdated = await this.backend.markJobFinished(this.id, this.attempt, JobState.Failed, result);
    if (isUpdated) {
      this._jobRecord.result = result;
      this._jobRecord.state = JobState.Failed;
      this._jobRecord.finishedAt = new Date();
      if (this.notifier.listenerCount('job_failed') > 0) {
        const event = {
          jobRecord: this.jobRecord,
          result: { ...result },
          duration: this.duration,
          job: this.job,
        };
        this.notifier.emit('job_failed', event);
      }
    }
    return isUpdated;
  }
}
