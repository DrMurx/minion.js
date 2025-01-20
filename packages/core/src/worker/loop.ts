import { type JobDequeueOptions } from '../types/backend.js';
import { type Job, type JobArgs } from '../types/job.js';
import { type WorkerInstance } from '../types/worker.js';
import { type Executor } from './executor.js';

/**
 * Encapsulates the management of all currently running jobs of a worker.
 */
export class WorkerLoop<BaseJob extends Job<JobArgs>> {
  /**
   * A list of currently running (or just finished) jobs
   */
  private jobs: JobStatus<BaseJob>[] = [];
  private stopPromises: Array<() => void> = [];

  constructor(protected worker: WorkerInstance<BaseJob>) {}

  /**
   * `true` if the loop has running jobs
   */
  get hasRunningJobs(): boolean {
    return this.jobs.length > 0;
  }

  /**
   * `true` if the worker's primary capacity is exhausted and it has only reserved capacity.
   */
  get hasOnlyReservedCapacity(): boolean {
    const { maxCapacity, reservedCapacity } = this.worker.config;
    return this.jobs.length >= maxCapacity - reservedCapacity && this.jobs.length < maxCapacity;
  }

  /**
   * `true` if the worker's full capacity is exhausted.
   */
  get hasNoCapacity(): boolean {
    const { maxCapacity } = this.worker.config;
    return this.jobs.length >= maxCapacity;
  }

  /**
   * `true` is a stop of this queue has been requested.
   */
  protected get isStopping(): boolean {
    return this.stopPromises.length > 0;
  }

  /**
   * Launch the main handler loop.
   */
  async run(): Promise<void> {
    while (!this.isStopping || this.hasRunningJobs) {
      await this.worker.processInbox();
      this.pruneFinished();
      await this.waitForCapacity();
      if (!this.isStopping) {
        await this.replenish();
      }
    }

    this.resolveStopPromises();
  }

  /**
   * Stop the main handler loop.
   */
  stop(): Promise<void> {
    return new Promise((resolve) => this.stopPromises.push(resolve));
  }

  /**
   * Filter `this.jobs` to only running jobs, and emit the number of finished jobs.
   */
  protected pruneFinished(): void {
    this.jobs = this.jobs.filter((jobStatus) => jobStatus.isRunning);
  }

  /**
   * If `this.jobs` is on its capacity, wait for one of the jobs to finish.
   */
  protected async waitForCapacity(): Promise<void> {
    if (this.hasNoCapacity) {
      await Promise.race(this.jobs.map((jobStatus) => jobStatus.performPromise));
    }
  }

  /**
   * Pull another job into the worker for execution. Should not be called when the worker has stopped.
   */
  protected async replenish(): Promise<boolean> {
    const { dequeueTimeout, queueNames, reservedMinPriority } = this.worker.config;

    // Dequeue options for pulling the job
    const options: JobDequeueOptions = {
      queueNames,
      // If only reserved slots are available, we fetch only jobs with configured min priority
      minPriority: this.hasOnlyReservedCapacity ? reservedMinPriority : undefined,
    };

    // Pull a job while assign it to current worker
    const executor = await this.worker.getNextExecutor(dequeueTimeout, options);
    if (executor === null) return false;

    // Construct the jobStatus object - the promise on `Job.perform` will update its status after it has finished
    const performPromise = executor.perform(false);
    const jobStatus: JobStatus<BaseJob> = {
      executor,
      performPromise,
      isRunning: true,
    };
    jobStatus.performPromise.finally(() => {
      jobStatus.isRunning = false;
    });
    this.jobs.push(jobStatus);

    return true;
  }

  /**
   * Resolve all stop promises.
   */
  protected resolveStopPromises(): void {
    const stop = this.stopPromises;
    this.stopPromises = [];
    stop.forEach((resolve) => resolve());
  }
}

interface JobStatus<BaseJob extends Job<JobArgs>> {
  executor: Executor<BaseJob>;
  performPromise: Promise<void>;
  isRunning: boolean;
}
