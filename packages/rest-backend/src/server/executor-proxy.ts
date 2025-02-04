import {
  InvalidStateError,
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
  protected _jobRecord: JobRecord<InferJobArgs<BaseJob>>;
  protected _worker: WorkerProxy<BaseJob>;
  protected _serial: number;
  protected _lastUpdateAt = Date.now();

  constructor(
    jobRecord: JobRecord<InferJobArgs<BaseJob>>,
    worker: WorkerProxy<BaseJob>,
    serial: number,
    protected backend: ExecutorBackend,
    protected notifier: QueueEventEmitter<BaseJob>,
  ) {
    this._jobRecord = jobRecord;
    this._worker = worker;
    this._serial = serial;
  }

  isExpired(expireAfter: number) {
    return this._lastUpdateAt < expireAfter;
  }

  get jobRecord(): Readonly<JobRecord<InferJobArgs<BaseJob>>> {
    return { ...this._jobRecord };
  }

  get serial(): number {
    return this._serial;
  }

  protected get id(): JobId {
    return this._jobRecord.id;
  }

  protected get state(): JobState {
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

  async updateProgress(progress: number): Promise<Readonly<JobRecord<InferJobArgs<BaseJob>>> | undefined> {
    if (progress <= this._jobRecord.progress) {
      return this._jobRecord;
    }
    const isUpdated = await this.backend.updateJobProgress(this._worker.id!, this.id, this.attempt, progress);
    if (isUpdated) {
      this._lastUpdateAt = Date.now();
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
      return this._jobRecord;
    }
  }

  async amendMetadata(records: Record<string, any>): Promise<Readonly<JobRecord<InferJobArgs<BaseJob>>> | undefined> {
    const metadata = await this.backend.amendJobMetadata(this._worker.id!, this.id, this.attempt, records);
    const isUpdated = metadata !== undefined;
    if (isUpdated) {
      this._jobRecord.metadata = metadata;
      this._lastUpdateAt = Date.now();
      return this._jobRecord;
    }
  }

  start(): void {
    this._jobRecord.state = JobState.Running;
    this._jobRecord.startedAt = new Date();
    this._jobRecord.finishedAt = undefined;

    if (this.notifier.listenerCount('job_started') > 0) {
      const event = {
        jobRecord: this.jobRecord,
      };
      this.notifier.emit('job_started', event);
    }

    this._lastUpdateAt = Date.now();
  }

  /**
   * Transition the job from `running` to `succeeded` or `failed`.
   */
  async markFinished(
    state: JobState.Succeeded | JobState.Failed,
    result: JobResult | JobError,
  ): Promise<Readonly<JobRecord<InferJobArgs<BaseJob>>> | undefined> {
    if (state !== JobState.Succeeded && state !== JobState.Failed) {
      throw new InvalidStateError(`Invalid state ${state}`);
    }

    if (state === JobState.Succeeded && !(await this.markSucceeded(result))) return;
    if (state === JobState.Failed && !(await this.markFailed(result))) return;

    if (this.notifier.listenerCount('job_finished') > 0) {
      const event = {
        jobRecord: this.jobRecord,
        state: this.state,
        duration: this.duration,
      };
      this.notifier.emit('job_finished', event);
    }
    return this._jobRecord;
  }

  /**
   * Transition from `running` to `succeeded` state with or without a result.
   */
  private async markSucceeded(result?: JobResult): Promise<boolean> {
    const isUpdated = await this.backend.markJobFinished(
      this._worker.id!,
      this.id,
      this.attempt,
      JobState.Succeeded,
      result ?? {},
    );
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
      this._lastUpdateAt = Date.now();
    }
    return isUpdated;
  }

  /**
   * Transition from `running` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `pending` with a delay based on the backoff policy.
   */
  private async markFailed(result: JobError): Promise<boolean> {
    const isUpdated = await this.backend.markJobFinished(
      this._worker.id!,
      this.id,
      this.attempt,
      JobState.Failed,
      result,
    );
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
      this._lastUpdateAt = Date.now();
    }
    return isUpdated;
  }
}
