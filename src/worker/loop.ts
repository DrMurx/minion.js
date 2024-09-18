import EventEmitter from 'events';
import { type Job, type JobDequeueOptions } from '../types/job.js';
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
    private worker: Worker,
    private queueReader: QueueReader,
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
      this.wipe();
      await this.waitToFinish();
      await this.replenish();
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
  protected wipe(): void {
    const before = this.jobs.length;
    this.jobs = this.jobs.filter((jobStatus) => jobStatus.isRunning);
    this.emit('finished', before - this.jobs.length);
  }

  /**
   * If `this.jobs` is on its capacity, wait for one of the jobs to finish.
   */
  protected async waitToFinish(): Promise<void> {
    const { concurrency, prefetchJobs } = this.worker.config;
    if (this.jobs.length >= concurrency + prefetchJobs) {
      await Promise.race(this.jobs.map((jobStatus) => jobStatus.promise));
    }
  }

  /**
   * Pull another job into the worker for execution, unless worker is stopped.
   */
  protected async replenish(): Promise<boolean> {
    if (this.isStopping) return false;

    const { dequeueTimeout, queues, concurrency, prefetchMinPriority } = this.worker.config;

    // Dequeue options for pulling the job
    const options: JobDequeueOptions = {
      queueNames: queues,
      // If regular concurrency slots are occupied, we fetch only jobs with configured min priority
      minPriority: this.jobs.length > concurrency ? prefetchMinPriority : undefined,
    };

    // Pull a job while assign it to current worker
    const job = await this.queueReader.assignNextJob(this.worker, dequeueTimeout, options);
    if (job === null) return false;

    // Construct the jobStatus object - the promise on `Job.perform` will update its status after it has finished
    const jobStatus: JobStatus = {
      job,
      isRunning: true,
      promise: job.perform(this.worker, false),
    };
    jobStatus.promise.then(() => {
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
  job: Job;
  isRunning: boolean;
  promise: Promise<void>;
}
