import type {Minion} from './minion.js';
import type {JobInfo, MinionArgs, MinionJob, MinionJobId, RetryOptions} from './types.js';

/**
 * Minion job class.
 */
export class Job {
  /**
   * Arguments passed to task.
   */
  args: MinionArgs;
  /**
   * Job id.
   */
  id: MinionJobId;
  /**
   * Number of times job has been retried.
   */
  retries: number;
  /**
   * Task name.
   */
  task: string;

  _isFinished = false;
  _minion: Minion;

  constructor(minion: Minion, id: MinionJobId, args: MinionArgs, retries: number, task: string) {
    this._minion = minion;
    this.id = id;
    this.args = args;
    this.retries = retries;
    this.task = task;
  }

  /**
   * Execute the appropriate task for job in this process. Note that this method should only be used to implement
   * custom workers.
   */
  async execute(): Promise<void> {
    try {
      const task = this.minion.tasks[this.task];
      await task(this, ...this.args);
    } finally {
      this._isFinished = true;
    }
  }

  /**
   * Transition from `active` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `inactive` with a delay based on `minion.backoff()`.
   */
  async fail(result: any = 'Unknown error'): Promise<boolean> {
    if (result instanceof Error) result = {name: result.name, message: result.message, stack: result.stack};
    return await this.minion.backend.failJob(this.id, this.retries, result);
  }

  /**
   * Transition from `active` to `finished` state with or without a result.
   */
  async finish(result?: any): Promise<boolean> {
    return await this.minion.backend.finishJob(this.id, this.retries, result);
  }

  /**
   * Get job information.
   */
  async info(): Promise<JobInfo | null> {
    const info = (await this.minion.backend.listJobs(0, 1, {ids: [this.id]})).jobs[0];
    return info === null ? null : info;
  }

  /**
   * Check if job has been executed in this process.
   */
  get isFinished(): boolean {
    return this._isFinished;
  }

  /**
   * Change one or more metadata fields for this job. Setting a value to `null` will remove the field. The new values
   * will get serialized as JSON.
   */
  async note(merge: Record<string, any>): Promise<boolean> {
    return await this.minion.backend.note(this.id, merge);
  }

  /**
   * Return all jobs this job depends on.
   */
  async parents(): Promise<MinionJob[]> {
    const results: MinionJob[] = [];

    const info = await this.info();
    if (info === null) return results;

    const minion = this.minion;
    for (const parent of info.parents) {
      const job = await minion.job(parent);
      if (job !== null) results.push(job);
    }

    return results;
  }

  /**
   * Perform job and wait for it to finish. Note that this method should only be used to implement custom workers.
   */
  async perform(): Promise<void> {
    try {
      await this.execute();
      await this.finish();
    } catch (error: any) {
      await this.fail(error);
    }
  }

  /**
   * Remove `failed`, `finished` or `inactive` job from queue.
   */
  async remove(): Promise<boolean> {
    return await this.minion.backend.removeJob(this.id);
  }

  /**
   * Transition job back to `inactive` state, already `inactive` jobs may also be retried to change options.
   */
  async retry(options: RetryOptions = {}) {
    return await this.minion.backend.retryJob(this.id, this.retries, options);
  }

  /**
   * Minion instance this job belongs to.
   */
  get minion(): Minion {
    return this._minion;
  }
}
