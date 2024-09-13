import type {Minion} from './minion.js';
import type {JobInfo, MinionArgs, MinionJob, MinionJobId, RetryOptions} from './types.js';

/**
 * Minion job class.
 */
export class Job {

  private _id: MinionJobId;
  private _args: MinionArgs;
  private _isFinished = false;

  /**
   *
   * @param minion Minion instance the job belongs to
   * @param id Job id
   * @param args Arguments passed to task
   * @param retries Number of times job has been retried
   * @param taskName Task name
   */
  constructor(
    private minion: Minion,
    id: MinionJobId,
    args: MinionArgs,
    public retries: number,
    public taskName: string
  ) {
    this._id = id;
    this._args = args;
  }

  get id(): MinionJobId {
    return this._id;
  }

  get args(): MinionArgs {
    return this._args;
  }

  /**
   * Perform job and wait for it to finish. Note that this method should only be used to implement custom workers.
   */
  async perform(): Promise<void> {
    try {
      await this.execute();
      await this.markFinished();
    } catch (error: any) {
      await this.markFailed(error);
    }
  }

  /**
   * Execute the appropriate task for job in this process. Note that this method should only be used to implement
   * custom workers.
   */
  async execute(): Promise<void> {
    try {
      const task = this.minion.tasks[this.taskName];
      await task(this, ...this._args);
    } finally {
      this._isFinished = true;
    }
  }

  /**
   * Transition from `active` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `inactive` with a delay based on `minion.backoff()`.
   */
  async markFailed(result: any = 'Unknown error'): Promise<boolean> {
    if (result instanceof Error) result = {name: result.name, message: result.message, stack: result.stack};
    return await this.minion.backend.markJobFailed(this._id, this.retries, result);
  }

  /**
   * Transition from `active` to `finished` state with or without a result.
   */
  async markFinished(result?: any): Promise<boolean> {
    return await this.minion.backend.markJobFinished(this._id, this.retries, result);
  }

  /**
   * Check if job has been executed in this process.
   */
  get isFinished(): boolean {
    return this._isFinished;
  }

  /**
   * Transition job back to `inactive` state, already `inactive` jobs may also be retried to change options.
   */
  async retry(options: RetryOptions = {}) {
    return await this.minion.backend.retryJob(this._id, this.retries, options);
  }

  /**
   * Remove `failed`, `finished` or `inactive` job from queue.
   */
  async remove(): Promise<boolean> {
    return await this.minion.backend.removeJob(this._id);
  }

  /**
   * Change one or more metadata fields for this job. Setting a value to `null` will remove the field. The new values
   * will get serialized as JSON.
   */
  async addNotes(notes: Record<string, any>): Promise<boolean> {
    return await this.minion.backend.addNotes(this._id, notes);
  }

  /**
   * Get job information.
   */
  async getInfo(): Promise<JobInfo | null> {
    const info = (await this.minion.backend.getJobInfos(0, 1, {ids: [this._id]})).jobs[0];
    return info === null ? null : info;
  }

  /**
   * Return all jobs this job depends on.
   */
  async getParentJobs(): Promise<MinionJob[]> {
    const results: MinionJob[] = [];

    const info = await this.getInfo();
    if (info === null) return results;

    const minion = this.minion;
    for (const parent of info.parents) {
      const job = await minion.getJob(parent);
      if (job !== null) results.push(job);
    }

    return results;
  }
}
