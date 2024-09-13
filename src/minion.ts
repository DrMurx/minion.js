import type {
  EnqueueOptions,
  DequeueOptions,
  JobInfo,
  ListJobsOptions,
  ListWorkersOptions,
  LockOptions,
  MinionArgs,
  MinionBackend,
  MinionHistory,
  MinionJob,
  MinionJobId,
  MinionOptions,
  MinionStats,
  MinionTask,
  MinionWorker,
  ResetOptions,
  WorkerInfo,
  WorkerOptions,
  ResultOptions
} from './types.js';
import {Job} from './job.js';
import {PgBackend} from './pg-backend.js';
import {Worker} from './worker.js';
import {BackendIterator} from './iterator.js';

export class AbortError extends Error {
  constructor(message?: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'AbortError';
  }
}

/**
 * Minion job queue class.
 */
export class Minion {
  /**
   * Backend, usually a PostgreSQL backend.
   */
  public backend: MinionBackend;

  /**
   * Amount of time in milliseconds after which workers without a heartbeat will be considered missing and removed from
   * the registry by `minion.repair()`, defaults to `1800000` (30 minutes).
   */
  public missingAfter = 1800000;

  /**
   * Amount of time in milliseconds after which jobs that have reached the state `finished` and have no unresolved
   * dependencies will be removed automatically by `minion.repair()`, defaults to `172800000` (2 days). It is not
   * recommended to set this value below 2 days.
   */
  public removeAfter = 172800000;

  /**
   * Amount of time in milliseconds after which jobs that have not been processed will be considered stuck by
   * `minion.repair()` and transition to the `failed` state, defaults to `172800000` (2 days).
   */
  public stuckAfter = 172800000;

  /**
   * Registered tasks.
   */
  public tasks: Record<string, MinionTask> = {};

  constructor(config: any, options: MinionOptions) {
    this.backend = new options.backendClass(this, config);

    if (options.missingAfter !== undefined) this.missingAfter = options.missingAfter;
    if (options.removeAfter !== undefined) this.removeAfter = options.removeAfter;
    if (options.stuckAfter !== undefined) this.stuckAfter = options.stuckAfter;
  }

  /**
   * Enqueue a new job with `inactive` state. Arguments get serialized by the backend as JSON, so you shouldn't send
   * objects that cannot be serialized, nested data structures are fine though.
   * @param options.attempts - Number of times performing this job will be attempted, with a delay based on
   *                           `minion.backoff()` after the first attempt, defaults to `1`.
   * @param options.delay - Delay job for this many milliseconds (from now), defaults to `0`.
   * @param options.expire - Job is valid for this many milliseconds (from now) before it expires.
   * @param options.lax - Existing jobs this job depends on may also have transitioned to the `failed` state to allow
   *                      for it to be processed, defaults to `false`.
   * @param options.notes - Object with arbitrary metadata for this job that gets serialized as JSON.
   * @param options.parents - One or more existing jobs this job depends on, and that need to have transitioned to the
   *                          state `finished` before it can be processed.
   * @param options.priority - Job priority, defaults to `0`. Jobs with a higher priority get performed first.
   *                           Priorities can be positive or negative, but should be in the range between `100` and
   *                           `-100`.
   * @param options.queue - Queue to put job in, defaults to `default`.
   */
  async addJob(task: string, args?: MinionArgs, options?: EnqueueOptions): Promise<MinionJobId> {
    return await this.backend.addJob(task, args, options);
  }

  /**
   * Return a promise for the future result of a job. The state `finished` will result in the promise being
   * `fullfilled`, and the state `failed` in the promise being `rejected`.
   */
  async getJobResult(id: MinionJobId, options: ResultOptions = {}): Promise<JobInfo | null> {
    const interval = options.interval ?? 3000;
    const signal = options.signal ?? null;
    return new Promise((resolve, reject) => this.waitForResult(id, interval, signal, resolve, reject));
  }

  /**
   * Get job object without making any changes to the actual job or return `null` if job does not exist.
   */
  async getJob(id: MinionJobId): Promise<MinionJob | null> {
    const info = (await this.backend.getJobInfos(0, 1, {ids: [id]})).jobs[0];
    return info === undefined ? null : new Job(this, info.id, info.args, info.retries, info.task);
  }

  /**
   * Return iterator object to safely iterate through job information.
   */
  getJobs(options: ListJobsOptions = {}): BackendIterator<JobInfo> {
    return new BackendIterator<JobInfo>(this, 'jobs', options);
  }

  /**
   * Get history information for job queue.
   */
  async getJobHistory(): Promise<MinionHistory> {
    return await this.backend.getJobHistory();
  }

  /**
   * Used to calculate the delay for automatically retried jobs, defaults to `(retries ** 4) + 15` (15, 16, 31, 96,
   * 271, 640...), which means that roughly `25` attempts can be made in `21` days.
   */
  calcBackoff(retries: number): number {
    return retries ** 4 + 15;
  }


  /**
   * Retry job in `minion_foreground` queue, then perform it right away with a temporary worker in this process,
   * very useful for debugging.
   */
  async runJob(id: number): Promise<boolean> {
    let job = await this.getJob(id);
    if (job === null) return false;
    if ((await job.retry({attempts: 1, queue: 'minion_foreground'})) !== true) return false;

    const worker = await this.createWorker().register();
    try {
      job = await worker.dequeue(0, {id, queues: ['minion_foreground']});
      if (job === null) return false;
      try {
        await job.execute();
        await job.markFinished();
        return true;
      } catch (error: any) {
        await job.markFailed(error);
        throw error;
      }
    } finally {
      await worker.unregister();
    }
  }

  /**
   * Perform all jobs with a temporary worker, very useful for testing.
   */
  async runJobs(options?: DequeueOptions): Promise<void> {
    const worker = await this.createWorker().register();
    try {
      let job;
      while ((job = await worker.register().then(worker => worker.dequeue(0, options)))) {
        await job.perform();
      }
    } finally {
      await worker.unregister();
    }
  }


  /**
   * Register a task.
   */
  addTask(name: string, fn: MinionTask): void {
    this.tasks[name] = fn;
  }


  /**
   * Build worker object. Note that this method should only be used to implement custom workers.
   */
  createWorker(options?: WorkerOptions): MinionWorker {
    return new Worker(this, options);
  }

  /**
   * Return iterator object to safely iterate through worker information.
   */
  getWorkers(options: ListWorkersOptions = {}): BackendIterator<WorkerInfo> {
    return new BackendIterator<WorkerInfo>(this, 'workers', options);
  }

  /**
   * Broadcast remote control command to one or more workers.
   */
  async notifyWorkers(command: string, args?: any[], ids?: number[]): Promise<boolean> {
    return await this.backend.notifyWorkers(command, args, ids);
  }


  /**
   * Try to acquire a named lock that will expire automatically after the given amount of time in milliseconds. You can
   * release the lock manually with `minion.unlock()` to limit concurrency, or let it expire for rate limiting.
   */
  async lock(name: string, duration: number, options?: LockOptions): Promise<boolean> {
    return await this.backend.lock(name, duration, options);
  }

  /**
   * Release a named lock that has been previously acquired with `minion.lock()`.
   */
  async unlock(name: string): Promise<boolean> {
    return await this.backend.unlock(name);
  }

  /**
   * Check if a lock with that name is currently active.
   */
  async isLocked(name: string): Promise<boolean> {
    return !(await this.backend.lock(name, 0));
  }


  /**
   * Get statistics for the job queue.
   */
  async stats(): Promise<MinionStats> {
    return await this.backend.stats();
  }

  /**
   * Repair worker registry and job queue if necessary.
   */
  async repair(): Promise<void> {
    await this.backend.repair();
  }

  /**
   * Update backend database schema to the latest version.
   */
  async updateSchema(): Promise<void> {
    await this.backend.updateSchema();
  }

  /**
   * Reset job queue.
   */
  async reset(options: ResetOptions): Promise<void> {
    await this.backend.reset(options);
  }

  /**
   * Stop using the queue.
   */
  async end(): Promise<void> {
    await this.backend.end();
  }


  protected async waitForResult(
    id: MinionJobId,
    interval: number,
    signal: AbortSignal | null,
    resolve: (value?: any) => void,
    reject: (reason?: any) => void
  ) {
    const rerun = () => this.waitForResult(id, interval, signal, resolve, reject);
    try {
      const info = (await this.backend.getJobInfos(0, 1, {ids: [id]})).jobs[0];
      if (info === undefined) {
        resolve(null);
      } else if (info.state === 'finished') {
        resolve(info);
      } else if (info.state === 'failed') {
        reject(info);
      } else if (signal !== null && signal.aborted === true) {
        reject(new AbortError());
      } else {
        setTimeout(rerun, interval);
      }
    } catch (error: any) {
      setTimeout(rerun, interval);
    }
  }
}
