import EventEmitter from 'events';
import { JobArgs, type Job, type JobDequeueOptions } from '../types/job.js';
import { type QueueReader } from '../types/queue.js';
import { type Worker } from '../types/worker.js';

/**
 * Encapsulates the management of all currently running jobs of a worker.
 */
export class WorkerLoop extends EventEmitter {
  /**
   * A list of currently running (or just finished) jobs
   */
  private jobs: JobStatus[] = [];
  private stopPromises: Array<() => void> = [];

  constructor(
    protected worker: Worker,
    protected queueReader: QueueReader,
  ) {
    super();
  }

  /**
   * `true` if the loop has running jobs
   */
  get hasRunningJobs(): boolean {
    return this.jobs.length > 0;
  }

  /**
   * `true` is a stop of this queue has been requested.
   */
  get isStopping(): boolean {
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
    const before = this.jobs.length;
    this.jobs = this.jobs.filter((jobStatus) => jobStatus.isRunning);
    this.emit('finished', before - this.jobs.length);
  }

  /**
   * If `this.jobs` is on its capacity, wait for one of the jobs to finish.
   */
  protected async waitForCapacity(): Promise<void> {
    const { concurrency, prefetchJobs } = this.worker.config;
    if (this.jobs.length >= concurrency + prefetchJobs) {
      await Promise.race(this.jobs.map((jobStatus) => jobStatus.performPromise));
    }
  }

  /**
   * Pull another job into the worker for execution. Should not be called when the worker has stopped.
   */
  protected async replenish(): Promise<boolean> {
    const { dequeueTimeout, queueNames, concurrency, prefetchMinPriority } = this.worker.config;

    // Dequeue options for pulling the job
    const options: JobDequeueOptions = {
      queueNames,
      // If regular concurrency slots are occupied, we fetch only jobs with configured min priority
      minPriority: this.jobs.length > concurrency ? prefetchMinPriority : undefined,
    };

    // Pull a job while assign it to current worker
    const job = await this.queueReader.assignNextJob(this.worker, dequeueTimeout, options);
    if (job === null) return false;

    // Construct the jobStatus object - the promise on `Job.perform` will update its status after it has finished
    const performPromise = job.perform(this.worker, false);
    const jobStatus: JobStatus = {
      job,
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

interface JobStatus {
  job: Job<JobArgs>;
  performPromise: Promise<void>;
  isRunning: boolean;
}
