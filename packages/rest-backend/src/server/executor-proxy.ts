import {
  JobState,
  type ExecutorBackend,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobError,
  type JobId,
  type JobRecord,
  type JobResult,
  type QueueEventEmitter,
} from '@queuebone/core';
import { WorkerProxy } from './worker-proxy.js';

/**
 * The server-side representation of a job executor on a REST worker.
 */
export class ExecutorProxy<BaseJob extends Job<JobArgs>> {
  private _jobRecord: JobRecord<InferJobArgs<BaseJob>>;
  private _worker: WorkerProxy<BaseJob>;

  constructor(
    jobRecord: JobRecord<InferJobArgs<BaseJob>>,
    worker: WorkerProxy<BaseJob>,
    private backend: ExecutorBackend,
    private notifier: QueueEventEmitter<BaseJob>,
  ) {
    this._jobRecord = jobRecord;
    this._worker = worker;
  }

  get jobRecord(): JobRecord<InferJobArgs<BaseJob>> {
    return { ...this._jobRecord };
  }

  get id(): JobId {
    return this._jobRecord.id;
  }

  get state(): JobState {
    return this._jobRecord.state;
  }

  get attempt(): number {
    return this._jobRecord.attempt;
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

  async updateProgress(progress: number): Promise<boolean> {
    const isUpdated = await this.backend.updateJobProgress(this.id, this.attempt, progress);
    if (isUpdated) {
      this._jobRecord.progress = progress;

      if (this.notifier.listenerCount('job_progress') > 0) {
        const event = {
          jobRecord: this.jobRecord,
          progress,
          duration: this.duration,
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

  async start(): Promise<void> {
    this._jobRecord.state = JobState.Running;
    this._jobRecord.startedAt = new Date();
    this._jobRecord.finishedAt = undefined;

    if (this.notifier.listenerCount('job_started') > 0) {
      const event = {
        jobRecord: this.jobRecord,
      };
      this.notifier.emit('job_started', event);
    }
  }

  /**
   * Transition the job from `running` to `succeeded` or `failed`.
   */
  async markFinished(state: JobState.Succeeded | JobState.Failed, result: JobResult | JobError): Promise<boolean> {
    this._worker.finishedJobCount++;
    if (state === JobState.Succeeded) {
      return await this.markSucceeded(result);
    }
    if (state === JobState.Failed) {
      return await this.markFailed(result);
    }
    throw new Error(`Invalid state ${state}`);
  }

  /**
   * Transition from `running` to `succeeded` state with or without a result.
   */
  private async markSucceeded(result?: JobResult): Promise<boolean> {
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
        };
        this.notifier.emit('job_succeeded', event);
      }
      if (this.notifier.listenerCount('job_finished') > 0) {
        const event = {
          jobRecord: this.jobRecord,
          state: this.state,
          duration: this.duration,
        };
        this.notifier.emit('job_finished', event);
      }
    }
    return isUpdated;
  }

  /**
   * Transition from `running` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `pending` with a delay based on the backoff policy.
   */
  private async markFailed(result: JobError): Promise<boolean> {
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
        };
        this.notifier.emit('job_failed', event);
      }
    }
    return isUpdated;
  }
}
