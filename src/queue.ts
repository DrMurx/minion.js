import { BackendIterator } from './backends/iterator.js';
import { DefaultJob } from './job.js';
import { DefaultTaskManager } from './task-manager.js';
import { type Backend } from './types/backend.js';
import {
  type Job,
  type JobArgs,
  type JobDequeueOptions,
  type JobDescriptor,
  type JobEnqueueOptions,
  type JobId,
  type JobInfo,
  type JobResultOptions,
  JobState,
  type ListJobsOptions,
  type QueueJobStatistics,
} from './types/job.js';
import { type PruneOptions, type Queue, type QueueOptions, type QueueReader, type QueueStats } from './types/queue.js';
import { type Task, type TaskManager } from './types/task.js';
import {
  type ListWorkersOptions,
  type Worker,
  type WorkerCommandArg,
  type WorkerInfo,
  type WorkerOptions,
  WorkerState,
} from './types/worker.js';
import { version } from './version.js';
import { DefaultWorker } from './worker.js';

export interface DefaultQueueInterface extends Queue, QueueReader {}

/**
 * Job queue class.
 */
export class DefaultQueue implements DefaultQueueInterface {
  public static readonly DEFAULT_OPTIONS: Readonly<QueueOptions> = Object.freeze({
    pruneInterval: 5 * 60 * 1000,
    workerMissingTimeout: 30 * 60 * 1000,
    jobRetentionPeriod: 2 * 24 * 60 * 60 * 1000,
    jobStuckTimeout: 2 * 24 * 60 * 60 * 1000,
  });

  private options: QueueOptions;

  private taskManager: TaskManager = new DefaultTaskManager();
  private pruneScheduler: NodeJS.Timeout | undefined;
  private lastPruneAt: number = 0;

  /**
   * @param backend
   * @param options
   */
  constructor(
    private backend: Backend,
    options: Partial<QueueOptions> = {},
  ) {
    this.options = { ...DefaultQueue.DEFAULT_OPTIONS, ...options };
    this.scheduleNextPrune();
  }

  async addJob(taskName: string, args?: JobArgs, options?: JobEnqueueOptions): Promise<JobId> {
    return await this.backend.addJob(taskName, args ?? {}, options ?? {});
  }

  async getJobResult(id: JobId, options: JobResultOptions = {}): Promise<JobInfo | null> {
    const interval = options.interval ?? 3000;
    const signal = options.signal ?? null;
    return new Promise((resolve, reject) => this.waitForResult(id, interval, signal, resolve, reject));
  }

  async cancelJob(id: JobId): Promise<void> {
    this.backend.cancelJob(id);
  }

  async getJob(id: JobId): Promise<Job | null> {
    const info = await this.getJobInfo(id);
    if (info === undefined) return null;
    return this.createJobObject(info);
  }

  async getJobs(options: ListJobsOptions): Promise<Job[]> {
    const results: Job[] = [];
    for await (const jobInfo of this.listJobInfos(options)) {
      results.push(this.createJobObject(jobInfo));
    }
    return results;
  }

  protected createJobObject(jobInfo: JobDescriptor | JobInfo): Job {
    return new DefaultJob(this, this.taskManager, this.backend, jobInfo);
  }

  async getJobInfo(jobId: JobId): Promise<JobInfo | undefined> {
    return (await this.backend.getJobInfos(0, 1, { ids: [jobId] })).jobs[0];
  }

  listJobInfos(options: ListJobsOptions = {}, chunkSize: number = 10): BackendIterator<JobInfo> {
    return new BackendIterator<JobInfo>(this.backend, 'jobs', options, { chunkSize });
  }

  async getJobStatistics(): Promise<QueueJobStatistics> {
    return await this.backend.getJobHistory();
  }

  async assignNextJob(worker: Worker, wait = 0, options: JobDequeueOptions = {}): Promise<Job | null> {
    const workerId = worker.id;
    if (workerId === undefined) return null;

    const taskNames = this.taskManager.getTaskNames();

    let dequeueJobInfo;
    for (let repeat = 1; ; repeat--) {
      dequeueJobInfo = await this.backend.assignNextJob(workerId, taskNames, options);
      if (wait === 0 || dequeueJobInfo !== null || repeat <= 0) break;
      await this.backend.awaitNewJobs(wait);
    }

    return dequeueJobInfo === null ? null : this.createJobObject(dequeueJobInfo);
  }

  async runJob(jobId: number): Promise<boolean> {
    let job = await this.getJob(jobId);
    if (job === null) return false;
    if ((await job.retry({ queueName: DefaultWorker.FOREGROUND_QUEUE })) !== true) return false;

    const worker = await this.getNewWorker().register();
    try {
      job = await this.assignNextJob(worker, 0, { id: jobId, queueNames: [DefaultWorker.FOREGROUND_QUEUE] });
      if (job === null) return false;
      await job.perform(worker, true);
      return true;
    } finally {
      await worker.unregister();
    }
  }

  async runJobs(options?: JobDequeueOptions): Promise<void> {
    const worker = await this.getNewWorker().register();
    try {
      let job: Job | null;
      while ((job = await worker.heartbeat().then((worker) => this.assignNextJob(worker, 0, options)))) {
        await job.perform(worker);
      }
    } finally {
      await worker.unregister();
    }
  }

  registerTask(taskName: string, fn: Task): void {
    this.taskManager.registerTask(taskName, fn);
  }

  getNewWorker(options?: WorkerOptions): Worker {
    return new DefaultWorker(this, this.backend, options ?? {});
  }

  listWorkerInfos(options: ListWorkersOptions = {}, chunkSize: number = 10): BackendIterator<WorkerInfo> {
    const _options: ListWorkersOptions = {
      state: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
      ...options,
    };
    return new BackendIterator<WorkerInfo>(this.backend, 'workers', _options, { chunkSize });
  }

  async sendWorkerCommand(command: string, arg: WorkerCommandArg, options?: ListWorkersOptions): Promise<boolean> {
    const _options: ListWorkersOptions = {
      state: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
      ...(options ?? {}),
    };
    return await this.backend.sendWorkerCommand(command, arg ?? {}, _options);
  }

  async prune(extraOptions: Partial<PruneOptions> = {}): Promise<boolean> {
    return await this.performPruneRun(true, extraOptions);
  }

  /**
   * Flag to indicate that the pruning scheduler is active.
   */
  protected get pruneSchedulerActive(): boolean {
    return this.pruneScheduler !== undefined;
  }

  /**
   * Schedules a prune in 60 seconds. An existing prune timer will be cleared.
   */
  protected scheduleNextPrune() {
    clearTimeout(this.pruneScheduler);
    this.pruneScheduler = setTimeout(async () => {
      await this.performPruneRun(false).catch((e) => console.error(e));
    }, 60 * 1000);
  }

  protected get needsPrune(): boolean {
    return this.lastPruneAt + this.options.pruneInterval < Date.now();
  }

  protected async performPruneRun(force: boolean, extraOptions: Partial<PruneOptions> = {}): Promise<boolean> {
    try {
      if (!force && !this.needsPrune) return false;

      const options = { ...this.options, ...extraOptions };

      const { deletedStaleWorkers } = await this.backend.pruneWorkers(options.workerMissingTimeout);
      const { deletedPendingJobs, abandonedJobs } = await this.backend.pruneJobs(
        options.jobStuckTimeout,
        options.jobRetentionPeriod,
        [DefaultWorker.FOREGROUND_QUEUE],
      );

      for (const jobDescriptor of abandonedJobs) {
        const job = this.createJobObject(jobDescriptor);
        await job.retryFailed();
      }

      this.lastPruneAt = Date.now();
      return deletedStaleWorkers.length > 0 || deletedPendingJobs.length > 0;
    } finally {
      if (this.pruneSchedulerActive) this.scheduleNextPrune();
    }
  }

  async getStatistics(): Promise<QueueStats> {
    const stats = await this.backend.getStats();

    stats.queueboneVersion = version;
    stats.backendName = this.backend.name;

    return stats;
  }

  async updateSchema(): Promise<void> {
    await this.backend.updateSchema();
  }

  async resetQueue(): Promise<void> {
    await this.backend.reset();
  }

  async end(): Promise<void> {
    clearTimeout(this.pruneScheduler);
    this.pruneScheduler = undefined;
    await this.backend.end();
  }

  protected async waitForResult(
    jobId: JobId,
    interval: number,
    signal: AbortSignal | null,
    resolve: (value?: any) => void,
    reject: (reason?: any) => void,
  ) {
    const rerun = () => this.waitForResult(jobId, interval, signal, resolve, reject);
    try {
      const info = await this.getJobInfo(jobId);
      if (info === undefined) {
        resolve(null);
      } else if (info.state === JobState.Succeeded) {
        resolve(info);
      } else if (
        info.state === JobState.Failed ||
        info.state === JobState.Aborted ||
        info.state === JobState.Abandoned ||
        info.state === JobState.Stuck ||
        info.state === JobState.Canceled
      ) {
        reject(info);
      } else if (signal !== null && signal.aborted === true) {
        reject(signal.reason);
      } else {
        setTimeout(rerun, interval);
      }
    } catch (_: any) {
      setTimeout(rerun, interval);
    }
  }
}
