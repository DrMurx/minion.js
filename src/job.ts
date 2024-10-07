import { type JobBackend } from './types/backend.js';
import {
  JobState,
  type Job,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  type JobRetryOptions,
} from './types/job.js';
import { type JobManager } from './types/queue.js';
import { type TaskReader } from './types/task.js';
import { type Worker } from './types/worker.js';

/**
 * Default job class.
 */
export class DefaultJob implements Job {
  private _state?: JobState = JobState.Pending;
  private _progress: number = 0.0;

  private _worker: Worker | null = null;
  private _abortController: AbortController = new AbortController();

  /**
   * @param jobManager Accessing additional job services
   * @param taskReader For access to the task vault
   * @param backend Queue backend
   * @param jobInfo Simplified JobInfo object
   */
  constructor(
    protected jobManager: JobManager,
    private taskReader: TaskReader,
    private backend: JobBackend,
    private readonly jobInfo: JobDescriptor | JobInfo,
  ) {
    if ('state' in jobInfo) {
      this._state = jobInfo.state;
    }
  }

  get id(): JobId {
    return this.jobInfo.id;
  }

  get taskName(): string {
    return this.jobInfo.taskName;
  }

  get args(): JobArgs {
    return this.jobInfo.args;
  }

  get state(): JobState | undefined {
    return this._state;
  }

  get progress(): number {
    return this._progress;
  }

  get maxAttempts(): number {
    return this.jobInfo.maxAttempts;
  }

  get attempt(): number {
    return this.jobInfo.attempt;
  }

  get abortSignal(): AbortSignal {
    return this._abortController.signal;
  }

  async perform(worker: Worker, throwOnError: boolean = false): Promise<void> {
    if (this._state !== JobState.Pending && this._state !== JobState.Scheduled && this._state !== JobState.Running) {
      throw new Error(`Try to perform job with state ${this._state}: ${this.id}`);
    }

    const abortEventHandler = (event: Event) => {
      if (event.type === 'abort') {
        this._abortController.abort(worker.abortSignal.reason);
      }
    };

    this._state = JobState.Running;
    this._progress = 0.0;
    this._worker = worker;

    try {
      worker.abortSignal.throwIfAborted();
      worker.abortSignal.addEventListener('abort', abortEventHandler);

      const task = this.taskReader.getTask(this.taskName);
      const result = await task.handle(this);
      await this.markSucceeded(result);
    } catch (error: any) {
      await this.markFailed(error);
      if (throwOnError) throw error;
    } finally {
      worker.abortSignal.removeEventListener('abort', abortEventHandler);
      this._worker = null;
    }
  }

  async updateProgress(progress: number): Promise<boolean> {
    const isUpdated = await this.backend.updateJobProgress(this.id, this.attempt, progress);
    if (isUpdated) {
      this._progress = progress;
      await this._worker?.heartbeat();
    }
    return isUpdated;
  }

  async amendMetadata(records: Record<string, any>): Promise<boolean> {
    return await this.backend.amendJobMetadata(this.id, records);
  }

  async markSucceeded(result?: JobResult): Promise<boolean> {
    const isUpdated = await this.backend.markJobFinished(JobState.Succeeded, this.id, this.attempt, result);
    if (isUpdated) {
      this._state = JobState.Succeeded;
      this._progress = 1.0;
    }
    return isUpdated;
  }

  async markFailed(result: any = 'Unknown error'): Promise<boolean> {
    if (result instanceof Error) result = { name: result.name, message: result.message, stack: result.stack };
    const isUpdated = await this.backend.markJobFinished(JobState.Failed, this.id, this.attempt, result);
    if (isUpdated) {
      this._state = JobState.Failed;
      await this.retryFailed();
    }
    return isUpdated;
  }

  async retry(options: JobRetryOptions = {}): Promise<boolean> {
    return await this.backend.retryJob(this.id, this.attempt, options);
  }

  async retryFailed(): Promise<boolean> {
    if (this.attempt >= this.maxAttempts) return false;
    const options = {
      maxAttempts: this.maxAttempts,
      delayFor: await this.getBackoffDelay(),
    };
    return await this.retry(options);
  }

  async cancel(): Promise<boolean> {
    const isUpdated = await this.backend.cancelJob(this.id);
    if (isUpdated) {
      this._state = JobState.Canceled;
    }
    return isUpdated;
  }

  async remove(): Promise<boolean> {
    const isUpdated = await this.backend.removeJob(this.id);
    if (isUpdated) {
      this._state = undefined;
    }
    return isUpdated;
  }

  async getInfo(): Promise<JobInfo | undefined> {
    const jobInfo = await this.jobManager.getJobInfo(this.id);
    if (jobInfo && jobInfo.attempt === this.attempt) {
      this._state = jobInfo.state;
    }
    return jobInfo;
  }

  async getParentJobs(): Promise<Job[]> {
    const info = await this.getInfo();
    if (info === undefined) return [];
    return await this.jobManager.getJobs({ ids: info.parentJobIds });
  }

  async getBackoffDelay(): Promise<number> {
    return this.attempt ** 4 + 15;
  }
}
