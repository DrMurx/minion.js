import { type JobBackend } from '../types/backend.js';
import {
  JobState,
  unsuccessfulJobStates,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDescriptor,
  type JobError,
  type JobId,
  type JobInfo,
  type JobResult,
} from '../types/job.js';
import { type JobFactory, type QueueEventEmitter } from '../types/queue.js';
import { type RunningWorker } from '../types/worker.js';

export class Executor<BaseJob extends Job<JobArgs>> {
  private jobInfo: ExecutorJobInfo<InferJobArgs<BaseJob>>;

  private _job?: BaseJob;
  private worker?: RunningWorker<BaseJob>;
  private abortController: AbortController = new AbortController();

  constructor(
    private backend: JobBackend,
    jobInfo: JobDescriptor<InferJobArgs<BaseJob>> | JobInfo<InferJobArgs<BaseJob>>,
    private jobFactory: JobFactory<BaseJob>,
    private notifier: QueueEventEmitter<BaseJob>,
  ) {
    this.jobInfo = {
      result: {},
      state: JobState.Running,
      progress: 0.0,
      metadata: {},
      ...jobInfo,
    };
  }

  get job(): BaseJob {
    if (!this._job) {
      this._job = this.jobFactory.createJobObject(this);
    }
    return this._job;
  }

  get id(): JobId {
    return this.jobInfo.id;
  }

  get taskName(): string {
    return this.jobInfo.taskName;
  }

  get args(): InferJobArgs<BaseJob> {
    return this.jobInfo.args;
  }

  get result(): JobResult | undefined {
    return this.jobInfo.result;
  }

  get state(): JobState {
    return this.jobInfo.state;
  }

  get progress(): number {
    return this.jobInfo.progress;
  }

  get maxAttempts(): number {
    return this.jobInfo.maxAttempts;
  }

  get attempt(): number {
    return this.jobInfo.attempt;
  }

  get startedAt(): Date | undefined {
    return this.jobInfo.startedAt;
  }

  get finishedAt(): Date | undefined {
    return this.jobInfo.finishedAt;
  }

  get abortSignal(): AbortSignal {
    return this.abortController.signal;
  }

  async updateProgress(progress: number): Promise<boolean> {
    const isUpdated = await this.backend.updateJobProgress(this.id, this.attempt, progress);
    if (isUpdated) {
      this.jobInfo.progress = progress;

      if (this.notifier.listenerCount('job_progress') > 0) {
        const event = {
          job: this.job,
          progress,
          duration: Date.now() - this.startedAt!.getTime(),
        };
        this.notifier.emit('job_progress', event);
      }

      if (this.worker) await this.worker.heartbeat();
    }
    return isUpdated;
  }

  async amendMetadata(records: Record<string, any>): Promise<boolean> {
    return await this.backend.amendJobMetadata(this.id, records);
  }

  /**
   * Perform job and wait for it to finish.
   */
  async perform(worker: RunningWorker<BaseJob>, throwOnError: boolean = false): Promise<void> {
    if (![JobState.Pending, JobState.Scheduled, JobState.Running].includes(this.state)) {
      throw new Error(`Try to perform job with state ${this.state}: ${this.id}`);
    }

    const abortEventHandler = (event: Event) => {
      if (event.type === 'abort') {
        this.abortController.abort(worker.abortSignal.reason);
      }
    };

    this.jobInfo.state = JobState.Running;
    this.jobInfo.startedAt = new Date();
    this.worker = worker;

    if (this.notifier.listenerCount('job_started') > 0) {
      const event = {
        job: this.job,
      };
      this.notifier.emit('job_started', event);
    }

    try {
      worker.abortSignal.throwIfAborted();
      worker.abortSignal.addEventListener('abort', abortEventHandler);

      const task = worker.getTask(this.taskName);
      const result = await task.handle(this.job, worker);
      await this.markSucceeded(result ?? {});
    } catch (error: any) {
      await this.markFailed(error);
      if (throwOnError) throw error;
    } finally {
      worker.abortSignal.removeEventListener('abort', abortEventHandler);
      this.worker = undefined;

      if (this.notifier.listenerCount('job_finished') > 0) {
        const event = {
          job: this.job,
          state: this.state,
          duration: Date.now() - this.startedAt!.getTime(),
        };
        this.notifier.emit('job_finished', event);
      }
    }
  }

  /**
   * Transition from `running` to `succeeded` state with or without a result.
   */
  async markSucceeded(result?: JobResult): Promise<boolean> {
    const isUpdated = await this.backend.markJobFinished(JobState.Succeeded, this.id, this.attempt, result ?? {});
    if (isUpdated) {
      this.jobInfo.state = JobState.Succeeded;
      this.jobInfo.progress = 1.0;
      this.jobInfo.finishedAt = new Date();
      if (this.notifier.listenerCount('job_succeeded') > 0) {
        const event = {
          job: this.job,
          result: { ...result },
          duration: this.finishedAt!.getTime() - this.startedAt!.getTime(),
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
    const isUpdated = await this.backend.markJobFinished(JobState.Failed, this.id, this.attempt, result);
    if (isUpdated) {
      this.jobInfo.state = JobState.Failed;
      this.jobInfo.finishedAt = new Date();
      if (this.notifier.listenerCount('job_failed') > 0) {
        const event = {
          job: this.job,
          result: { ...result },
          duration: this.finishedAt!.getTime() - this.startedAt!.getTime(),
        };
        this.notifier.emit('job_failed', event);
      }

      await this.retryFailed();
    }
    return isUpdated;
  }

  /**
   * Transition a `failed` job back to `pending` or `scheduled` state if there are still attempts left.
   */
  async retryFailed(): Promise<void> {
    if (unsuccessfulJobStates.includes(this.state) && this.attempt < this.maxAttempts) {
      const options = {
        // Set maxAttempt to its current value (otherwise, `Backend.retryJob` increases it)
        maxAttempts: this.maxAttempts,
        delayFor: await this.job.getBackoffDelay(),
      };
      await this.backend.retryJob<InferJobArgs<BaseJob>>(this.id, this.attempt, options);
    }
  }
}

interface ExecutorJobInfo<Args extends JobArgs = JobArgs> {
  id: JobId;

  taskName: string;
  args: Args;
  result: JobResult;

  state: JobState;
  progress: number;
  maxAttempts: number;
  attempt: number;

  metadata: Record<string, any>;

  startedAt?: Date;
  finishedAt?: Date;
}
