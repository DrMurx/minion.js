import EventEmitter from 'events';
import { WorkerPruneResult, type Backend, type JobPruneResult } from '../types/backend.js';
import { type JobDescriptor } from '../types/job.js';
import { type JobFactory, type PruneOptions } from '../types/queue.js';

export class QueuePruner {
  private enabled: boolean = false;
  private performPromise: Promise<boolean> | undefined;
  private pruneScheduler: NodeJS.Timeout | undefined;
  private lastPruneAt: number = 0;

  constructor(
    private queue: EventEmitter,
    private jobFactory: JobFactory<any>,
    private backend: Backend,
    private pruneInterval: number,
    private options: PruneOptions,
  ) {}

  /**
   * Ensures the prune cycle
   */
  start(): this {
    this.enabled = true;
    this.scheduleNext(0);
    return this;
  }

  async stop(): Promise<void> {
    clearTimeout(this.pruneScheduler);
    this.enabled = false;
    this.pruneScheduler = undefined;
  }

  /**
   * Flag to indicate that the pruning scheduler is enabled.
   */
  get isRunning(): boolean {
    return this.enabled;
  }

  protected scheduleNext(ms: number): void {
    clearTimeout(this.pruneScheduler);
    this.pruneScheduler = setTimeout(() => this.perform(false), ms);
  }

  protected get needsPrune(): boolean {
    return this.lastPruneAt + this.pruneInterval < Date.now();
  }

  async perform(force: boolean, extraOptions: Partial<PruneOptions> = {}): Promise<boolean> {
    if (this.performPromise) return this.performPromise;
    clearTimeout(this.pruneScheduler);

    this.performPromise = (async (): Promise<boolean> => {
      try {
        if (!force && !this.needsPrune) return false;

        const options = { ...this.options, ...extraOptions };
        const workerPruneResult = await this.backend.pruneWorkers(options.workerLostTimeout);
        const jobPruneResult = await this.backend.pruneJobs<any>(options.jobUnattendedPeriod, options.jobExpungePeriod);
        this.sendPruneNotifications(workerPruneResult, jobPruneResult);
        await this.retryFailed(jobPruneResult.abandonedJobs);

        this.lastPruneAt = Date.now();
        return workerPruneResult.lostWorkers.length > 0 || jobPruneResult.expiredJobs.length > 0;
      } catch (error) {
        console.error(error);
        return false;
      } finally {
        this.performPromise = undefined;
        if (this.enabled) this.scheduleNext(Date.now() - this.lastPruneAt + this.pruneInterval + 1);
      }
    })();
    return this.performPromise;
  }

  protected async retryFailed(abandonedJobs: JobDescriptor<any>[]) {
    for (const jobDescriptor of abandonedJobs) {
      const job = this.jobFactory.createJobObject(jobDescriptor);
      await job.retryFailed();
    }
  }

  protected sendPruneNotifications(workerPruneResult: WorkerPruneResult, jobPruneResult: JobPruneResult<any>) {
    const { lostWorkers } = workerPruneResult;
    if (this.queue.listenerCount('worker_lost') > 0) {
      for (const lostWorker of lostWorkers) {
        this.queue.emit('worker_lost', { worker: lostWorker });
      }
    }

    const { expiredJobs, expungedJobs, abandonedJobs, unattendedJobs } = jobPruneResult;
    if (this.queue.listenerCount('job_expired') > 0) {
      for (const job of expiredJobs) {
        this.queue.emit('job_expired', { job });
      }
    }
    if (this.queue.listenerCount('job_expunged') > 0) {
      for (const job of expungedJobs) {
        this.queue.emit('job_expunged', { job });
      }
    }
    if (this.queue.listenerCount('job_abandoned') > 0) {
      for (const job of abandonedJobs) {
        this.queue.emit('job_abandoned', { job });
      }
    }
    if (this.queue.listenerCount('job_unattended') > 0) {
      for (const job of unattendedJobs) {
        this.queue.emit('job_unattended', { job });
      }
    }
  }
}
