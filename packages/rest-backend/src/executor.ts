import {
  JobState,
  type ExecutorBackend,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobError,
  type JobId,
  type JobInfo,
  type JobResult,
  type QueueEventEmitter,
} from '@queuebone/core';

export class Executor<BaseJob extends Job<JobArgs>> {
  private _jobInfo: JobInfo<InferJobArgs<BaseJob>>;

  constructor(
    jobInfo: JobInfo<InferJobArgs<BaseJob>>,
    private backend: ExecutorBackend,
    private notifier: QueueEventEmitter<BaseJob>,
  ) {
    this._jobInfo = jobInfo;
  }

  get jobInfo(): JobInfo<InferJobArgs<BaseJob>> {
    return { ...this._jobInfo };
  }

  get id(): JobId {
    return this._jobInfo.id;
  }

  get state(): JobState {
    return this._jobInfo.state;
  }

  get attempt(): number {
    return this._jobInfo.attempt;
  }

  get startedAt(): Date | undefined {
    return this._jobInfo.startedAt;
  }

  get duration(): number {
    if (this._jobInfo.startedAt) {
      if (this._jobInfo.finishedAt) {
        return this._jobInfo.finishedAt.getTime() - this._jobInfo.startedAt.getTime();
      } else {
        return Date.now() - this._jobInfo.startedAt.getTime();
      }
    }
    return 0;
  }

  async updateProgress(progress: number): Promise<boolean> {
    const isUpdated = await this.backend.updateJobProgress(this.id, this.attempt, progress);
    if (isUpdated) {
      this._jobInfo.progress = progress;

      if (this.notifier.listenerCount('job_progress') > 0) {
        const event = {
          jobInfo: this.jobInfo,
          progress,
          duration: Date.now() - this.startedAt!.getTime(),
        };
        this.notifier.emit('job_progress', event);
      }

      // await this._worker.heartbeat();
    }
    return isUpdated;
  }

  async amendMetadata(records: Record<string, any>): Promise<boolean> {
    const metadata = await this.backend.amendJobMetadata(this.id, this.attempt, records);
    const isUpdated = metadata !== undefined;
    if (isUpdated) {
      this._jobInfo.metadata = metadata;
    }
    return isUpdated;
  }

  async start(): Promise<void> {
    this._jobInfo.state = JobState.Running;
    this._jobInfo.startedAt = new Date();
    this._jobInfo.finishedAt = undefined;

    if (this.notifier.listenerCount('job_started') > 0) {
      const event = {
        jobInfo: this.jobInfo,
      };
      this.notifier.emit('job_started', event);
    }
  }

  /**
   * Transition the job from `running` to `succeeded` or `failed`. The
   * @param state
   * @param result
   */
  async markFinished(state: JobState.Succeeded | JobState.Failed, result: JobResult | JobError): Promise<boolean> {
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
      this._jobInfo.result = result;
      this._jobInfo.state = JobState.Succeeded;
      this._jobInfo.progress = 1.0;
      this._jobInfo.finishedAt = new Date();
      if (this.notifier.listenerCount('job_succeeded') > 0) {
        const event = {
          jobInfo: this.jobInfo,
          result: { ...result },
          duration: this.duration,
        };
        this.notifier.emit('job_succeeded', event);
      }
      if (this.notifier.listenerCount('job_finished') > 0) {
        const event = {
          jobInfo: this.jobInfo,
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
      this._jobInfo.result = result;
      this._jobInfo.state = JobState.Failed;
      this._jobInfo.finishedAt = new Date();
      if (this.notifier.listenerCount('job_failed') > 0) {
        const event = {
          jobInfo: this.jobInfo,
          result: { ...result },
          duration: this.duration,
        };
        this.notifier.emit('job_failed', event);
      }
    }
    return isUpdated;
  }
}
