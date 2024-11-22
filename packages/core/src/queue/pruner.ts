import { type Backend, type JobPruneResult, type WorkerPruneResult } from '../types/backend.js';
import { type InferJobArgs, type Job, type JobArgs } from '../types/job.js';
import { type PruneOptions, type QueueEventEmitter } from '../types/queue.js';

export class QueuePruner<BaseJob extends Job<JobArgs>> {
  private enabled: boolean = false;
  private performPromise: Promise<boolean> | undefined;
  private pruneScheduler: NodeJS.Timeout | undefined;
  private lastPruneAt: number = 0;

  constructor(
    private backend: Backend,
    private options: Readonly<PruneOptions>,
    private ignoreQueues: string[],
    private notifier: QueueEventEmitter<BaseJob>,
  ) {}

  /**
   * Ensures the prune cycle
   */
  start(): this {
    this.enabled = true;
    this.scheduleNext(true);
    return this;
  }

  async stop(): Promise<void> {
    clearTimeout(this.pruneScheduler);
    this.enabled = false;
    this.pruneScheduler = undefined;
    if (this.performPromise) await this.performPromise;
  }

  /**
   * Flag to indicate that the pruning scheduler is enabled.
   */
  get isRunning(): boolean {
    return this.enabled;
  }

  protected scheduleNext(immediate?: boolean): void {
    if (!this.enabled) return;

    let ms = 1;
    if (!immediate) {
      const extraOffset = this.lastPruneAt === 0 ? 0 : Date.now() - this.lastPruneAt;
      ms = Math.max(1, this.options.pruneInterval - extraOffset + 1);
    }

    clearTimeout(this.pruneScheduler);
    this.pruneScheduler = setTimeout(() => this.perform(false), ms);
  }

  protected get needsPrune(): boolean {
    return this.lastPruneAt + this.options.pruneInterval < Date.now();
  }

  async perform(force: boolean, extraOptions: Partial<PruneOptions> = {}): Promise<boolean> {
    if (this.performPromise) return this.performPromise;

    clearTimeout(this.pruneScheduler);

    if (!force && !this.needsPrune) {
      this.scheduleNext();
      return false;
    }

    const options = { ...this.options, ...extraOptions };
    this.notifier.emit('prune_run', { force, options });

    this.performPromise = (async (): Promise<boolean> => {
      try {
        const workerPruneResult = await this.backend.pruneWorkers(options.workerLostTimeout);
        const jobPruneResult = await this.backend.pruneJobs<InferJobArgs<BaseJob>>(
          options.jobUnattendedPeriod,
          options.jobExpungePeriod,
          this.ignoreQueues,
        );
        this.sendPruneNotifications(workerPruneResult, jobPruneResult);
        this.lastPruneAt = Date.now();
        return workerPruneResult.lostWorkers.length > 0 || jobPruneResult.expiredJobs.length > 0;
      } catch (error) {
        console.error(error);
        return false;
      } finally {
        this.performPromise = undefined;
        this.scheduleNext();
      }
    })();

    return this.performPromise;
  }

  protected sendPruneNotifications(workerPruneResult: WorkerPruneResult, jobPruneResult: JobPruneResult<any>) {
    const { lostWorkers } = workerPruneResult;
    if (this.notifier.listenerCount('worker_lost') > 0) {
      for (const lostWorker of lostWorkers) {
        this.notifier.emit('worker_lost', { workerInfo: lostWorker });
      }
    }

    const { expiredJobs, expungedJobs, abandonedJobs, unattendedJobs } = jobPruneResult;
    if (this.notifier.listenerCount('job_expired') > 0) {
      for (const jobRecord of expiredJobs) {
        this.notifier.emit('job_expired', { jobRecord });
      }
    }
    if (this.notifier.listenerCount('job_expunged') > 0) {
      for (const jobRecord of expungedJobs) {
        this.notifier.emit('job_expunged', { jobRecord });
      }
    }
    if (this.notifier.listenerCount('job_abandoned') > 0) {
      for (const jobRecord of abandonedJobs) {
        this.notifier.emit('job_abandoned', { jobRecord });
      }
    }
    if (this.notifier.listenerCount('job_unattended') > 0) {
      for (const jobRecord of unattendedJobs) {
        this.notifier.emit('job_unattended', { jobRecord });
      }
    }
  }
}
