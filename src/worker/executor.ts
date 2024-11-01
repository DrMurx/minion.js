import { type JobBackend } from '../types/backend.js';
import {
  JobState,
  unsuccessfulJobStates,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobResult,
} from '../types/job.js';
import { type JobFactory } from '../types/queue.js';
import { type RunningWorker } from '../types/worker.js';

export class Executor<BaseJob extends Job<JobArgs>> {
  private _jobInfo: JobDescriptor<InferJobArgs<BaseJob>>;
  private _state: JobState;
  private _progress: number = 0.0;

  private _job?: BaseJob;
  private _worker?: RunningWorker<BaseJob>;
  private _abortController: AbortController = new AbortController();

  constructor(
    jobInfo: JobDescriptor<InferJobArgs<BaseJob>>,
    initialState: JobState,
    private jobFactory: JobFactory<BaseJob>,
    private backend: JobBackend,
  ) {
    this._jobInfo = jobInfo;
    this._state = initialState;
  }

  get job(): BaseJob {
    if (!this._job) {
      this._job = this.jobFactory.createJobObject(this);
    }
    return this._job;
  }

  get jobInfo(): JobDescriptor<InferJobArgs<BaseJob>> {
    return this._jobInfo;
  }

  get id(): JobId {
    return this._jobInfo.id;
  }

  get taskName(): string {
    return this._jobInfo.taskName;
  }

  get args(): InferJobArgs<BaseJob> {
    return this._jobInfo.args;
  }

  get maxAttempts(): number {
    return this._jobInfo.maxAttempts;
  }

  get attempt(): number {
    return this._jobInfo.attempt;
  }

  get state(): JobState {
    return this._state;
  }

  get progress(): number {
    return this._progress;
  }

  get abortSignal(): AbortSignal {
    return this._abortController.signal;
  }

  async updateProgress(progress: number): Promise<boolean> {
    const isUpdated = await this.backend.updateJobProgress(this.id, this.attempt, progress);
    if (isUpdated) {
      this._progress = progress;
      if (this._worker) await this._worker.heartbeat();
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
    if (this._state !== JobState.Pending && this._state !== JobState.Scheduled && this._state !== JobState.Running) {
      throw new Error(`Try to perform job with state ${this._state}: ${this.id}`);
    }

    const abortEventHandler = (event: Event) => {
      if (event.type === 'abort') {
        this._abortController.abort(worker.abortSignal.reason);
      }
    };

    this._state = JobState.Running;
    this._worker = worker;

    try {
      worker.abortSignal.throwIfAborted();
      worker.abortSignal.addEventListener('abort', abortEventHandler);

      const task = worker.getTask(this._jobInfo.taskName);
      const result = await task.handle(this.job, worker);
      await this.markSucceeded(result ?? {});
    } catch (error: any) {
      await this.markFailed(error);
      if (throwOnError) throw error;
    } finally {
      this._worker = undefined;
      worker.abortSignal.removeEventListener('abort', abortEventHandler);
    }
  }

  /**
   * Transition from `running` to `succeeded` state with or without a result.
   */
  async markSucceeded(result?: JobResult): Promise<boolean> {
    const isUpdated = await this.backend.markJobFinished(JobState.Succeeded, this.id, this.attempt, result ?? {});
    if (isUpdated) {
      this._progress = 1.0;
      this._state = JobState.Succeeded;
    }
    return isUpdated;
  }

  /**
   * Transition from `running` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `pending` with a delay based on the backoff policy.
   */
  async markFailed(result: JobResult | Error = new Error('Unknown error')): Promise<boolean> {
    if (result instanceof Error) {
      result = { name: result.name, message: result.message, stack: result.stack };
    }
    const isUpdated = await this.backend.markJobFinished(JobState.Failed, this.id, this.attempt, result);
    if (isUpdated) {
      this._state = JobState.Failed;
      await this.retryFailed();
    }
    return isUpdated;
  }

  /**
   * Transition a `failed` job back to `pending` or `scheduled` state if there are still attempts left.
   */
  async retryFailed(): Promise<void> {
    if (unsuccessfulJobStates.includes(this._state) && this.attempt < this.maxAttempts) {
      const options = {
        // Set maxAttempt to its current value (otherwise, `Backend.retryJob` increases it)
        maxAttempts: this.maxAttempts,
        delayFor: await this.job.getBackoffDelay(),
      };
      await this.backend.retryJob<InferJobArgs<BaseJob>>(this.id, this.attempt, options);
    }
  }
}
