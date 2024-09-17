import { BackendIterator } from './iterator.js';
import { Job } from './job.js';
import type {
  DequeueOptions,
  EnqueueOptions,
  JobInfo,
  ListJobsOptions,
  ListWorkersOptions,
  MinionArgs,
  MinionBackend,
  MinionBackoffStrategy,
  MinionHistory,
  MinionJob,
  MinionJobId,
  MinionOptions,
  MinionStats,
  MinionTask,
  MinionWorker,
  RepairOptions,
  ResetOptions,
  ResultOptions,
  WorkerInfo,
  WorkerOptions
} from './types.js';
import { Worker } from './worker.js';

export class AbortError extends Error {
  constructor(message?: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'AbortError';
  }
}

/**
 * The Minion default backoff strategy
 * @param retries
 * @returns
 */
export const defaultBackoffStrategy: MinionBackoffStrategy = (retries) => retries ** 4 + 15;

/**
 * Minion job queue class.
 */
export class Minion {
  /**
   * Registered tasks.
   */
  public tasks: Record<string, MinionTask> = {};

  private _repairOptions: RepairOptions;

  /**
   *
   * @param backend
   * @param options
   */
  constructor(public backend: MinionBackend, options: MinionOptions = {}) {
    this._repairOptions = {
      missingAfter: options.missingAfter ?? 1800000,
      removeAfter: options.removeAfter ?? 172800000,
      stuckAfter: options.stuckAfter ?? 172800000,
    };
  }

  /**
   * Enqueue a new job with `pending` state. Arguments get serialized by the backend as JSON, so you shouldn't send
   * objects that cannot be serialized, nested data structures are fine though.
   * @param options.attempts - Number of times performing this job will be attempted, with a delay based on
   *                           `minion.backoff()` after the first attempt, defaults to `1`.
   * @param options.delay - Delay job for this many milliseconds (from now), defaults to `0`.
   * @param options.expire - Job is valid for this many milliseconds (from now) before it expires.
   * @param options.lax - Existing jobs this job depends on may also have transitioned to the `failed` state to allow
   *                      for it to be processed, defaults to `false`.
   * @param options.notes - Object with arbitrary metadata for this job that gets serialized as JSON.
   * @param options.parents - One or more existing jobs this job depends on, and that need to have transitioned to the
   *                          state `succeeded` before it can be processed.
   * @param options.priority - Job priority, defaults to `0`. Jobs with a higher priority get performed first.
   *                           Priorities can be positive or negative, but should be in the range between `100` and
   *                           `-100`.
   * @param options.queue - Queue to put job in, defaults to `default`.
   */
  async addJob(taskName: string, args?: MinionArgs, options?: EnqueueOptions): Promise<MinionJobId> {
    return await this.backend.addJob(taskName, args, options);
  }

  /**
   * Return a promise for the future result of a job. The state `succeeded` will result in the promise being
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
    return info === undefined ? null : new Job(this, info.id, info.args, info.retries, info.taskName);
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
   * Retry job in `minion_foreground` queue, then perform it right away with a temporary worker in this process,
   * very useful for debugging.
   */
  async runJob(id: number): Promise<boolean> {
    let job = await this.getJob(id);
    if (job === null) return false;
    if ((await job.retry({attempts: 1, queueName: 'minion_foreground'})) !== true) return false;

    const worker = await this.createWorker().register();
    try {
      job = await worker.dequeue(0, {id, queueNames: ['minion_foreground']});
      if (job === null) return false;
      try {
        await job.execute();
        await job.markSucceeded();
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
  addTask(taskName: string, fn: MinionTask): void {
    this.tasks[taskName] = fn;
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
   * Get statistics for the job queue.
   */
  async stats(): Promise<MinionStats> {
    return await this.backend.stats();
  }

  get repairOptions(): Readonly<RepairOptions> {
    return Object.freeze({...this._repairOptions});
  }

  /**
   * Repair worker registry and job queue if necessary.
   */
  async repair(repairOptionsOverride: Partial<RepairOptions> = {}): Promise<void> {
    await this.backend.repair({...this._repairOptions, ...repairOptionsOverride});
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
      } else if (info.state === 'succeeded') {
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
