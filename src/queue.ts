import EventEmitter from 'events';
import { BackendIterator } from './backends/iterator.js';
import { DefaultJob } from './job.js';
import { DefaultTaskManager } from './task-manager.js';
import { type Backend, type JobDequeueOptions, type JobEnqueueOptions } from './types/backend.js';
import {
  type Job,
  type JobAddOptions,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  type JobResultOptions,
  JobState,
  type ListJobsOptions,
  type QueueJobStatistics,
} from './types/job.js';
import { type PruneOptions, type Queue, type QueueOptions, type QueueReader, type QueueStats } from './types/queue.js';
import { type Task, type TaskHandlerFunction, type TaskManager } from './types/task.js';
import {
  type ListWorkersOptions,
  type Worker,
  type WorkerCommandArg,
  WorkerConfig,
  type WorkerInfo,
  type WorkerOptions,
  WorkerState,
} from './types/worker.js';
import { version } from './version.js';
import { DefaultWorker } from './worker.js';

export interface DefaultQueueInterface<Args extends JobArgs = JobArgs> extends Queue<Args>, QueueReader {}

/**
 * Job queue class.
 */
export class DefaultQueue<Args extends JobArgs = JobArgs, ArgsJob extends Job<Args> = Job<Args>>
  extends EventEmitter
  implements DefaultQueueInterface<Args>
{
  public static readonly DEFAULT_OPTIONS = Object.freeze(<QueueOptions>{
    queueNames: ['default'],
    pruneInterval: 5 * 60 * 1000,
    workerLostTimeout: 30 * 60 * 1000,
    jobExpungePeriod: 2 * 24 * 60 * 60 * 1000,
    jobUnattendedPeriod: 2 * 24 * 60 * 60 * 1000,
  });

  private options: QueueOptions;

  protected taskManager: TaskManager<Args> = new DefaultTaskManager<Args>();
  private pruneScheduler: NodeJS.Timeout | undefined;
  private lastPruneAt: number = 0;

  /**
   * @param backend
   * @param options
   */
  constructor(
    protected backend: Backend,
    options: Partial<QueueOptions> = {},
  ) {
    super();
    this.options = { ...DefaultQueue.DEFAULT_OPTIONS, ...options };
    if (!Array.isArray(this.options.queueNames) || this.options.queueNames.length === 0) {
      throw new Error('No queue names given');
    }
    this.scheduleNextPrune();
  }

  async addJob<Args1 extends Args>(taskName: string, args?: Args1, options?: JobAddOptions): Promise<Job<Args1>> {
    const _args = args ?? ({} as Args1);
    const _options = <JobEnqueueOptions>{
      queueName: this.options.queueNames[0],
      priority: 0,
      maxAttempts: 1,
      parentJobIds: [],
      laxDependency: false,
      metadata: {},
      delayFor: 0,
      ...options,
    };
    const id = await this.backend.addJob(taskName, _args, _options);
    return this.createJobObject<Args1>({
      id,
      taskName,
      args: _args,
      maxAttempts: options?.maxAttempts ?? 1,
      attempt: 1,
    });
  }

  async addJobWithAck<Args1 extends Args>(
    taskName: string,
    args?: Args1,
    enqueueOptions?: JobAddOptions,
    resultOptions?: JobResultOptions,
  ): Promise<JobResult> {
    const job = await this.addJob(taskName, args, enqueueOptions);
    return await this.getJobResult(job.id, resultOptions ?? {});
  }

  async getJobResult(id: JobId, options: JobResultOptions = {}): Promise<JobResult> {
    const interval = options.interval ?? 3000;
    const signal = options.signal ?? null;
    return new Promise((resolve, reject) => this.waitForResult(id, interval, signal, resolve, reject));
  }

  async cancelJob(id: JobId): Promise<void> {
    this.backend.cancelJob(id);
  }

  async getJob<Args1 extends Args = Args, Args1Job extends Job<Args1> = Job<Args1>>(
    id: JobId,
  ): Promise<Args1Job | null> {
    const info = await this.getJobInfo<Args1>(id);
    if (info === undefined) return null;
    return this.createJobObject<Args1, Args1Job>(info);
  }

  async getJobs<Args1 extends Args = Args, Args1Job extends Job<Args1> = Job<Args1>>(
    options: ListJobsOptions,
  ): Promise<Args1Job[]> {
    const jobs: Args1Job[] = [];
    for await (const jobInfo of this.listJobInfos(options)) {
      jobs.push(this.createJobObject(jobInfo));
    }
    return jobs;
  }

  protected createJobObject<Args1 extends Args = Args, Args1Job extends Job<Args1> = Job<Args1>>(
    jobInfo: JobDescriptor<Args1> | JobInfo<Args1>,
  ): Args1Job {
    return new DefaultJob<Args>(this, this.taskManager, this.backend, jobInfo) as unknown as Args1Job;
  }

  async getJobInfo<Args1 extends Args = Args>(jobId: JobId): Promise<JobInfo<Args1> | undefined> {
    return await this.backend.getJobInfo(jobId);
  }

  listJobInfos(options: ListJobsOptions = {}, chunkSize: number = 10): BackendIterator<JobInfo<Args>> {
    return new BackendIterator<JobInfo<Args>>(this.backend, 'jobs', options, { chunkSize });
  }

  async getJobStatistics(): Promise<QueueJobStatistics> {
    return await this.backend.getJobHistory();
  }

  async assignNextJob(worker: Worker, wait = 0, options: Partial<JobDequeueOptions> = {}): Promise<ArgsJob | null> {
    if (worker.id === undefined) return null;
    const _options = <JobDequeueOptions>{
      queueNames: this.options.queueNames,
      ...options,
    };
    const taskNames = this.taskManager.getTaskNames();
    const dequeueJobInfo = await this.backend.assignNextJob<Args>(worker.id, taskNames, wait, _options);
    return dequeueJobInfo === null ? null : this.createJobObject<Args, ArgsJob>(dequeueJobInfo);
  }

  async runJob(jobId: number): Promise<boolean> {
    let job = await this.getJob(jobId);
    if (job === null) return false;
    if ((await job.retry({ queueName: DefaultWorker.FOREGROUND_QUEUE, maxAttempts: job.maxAttempts + 1 })) !== true)
      return false;

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
      let job: Job<JobArgs> | null;
      while ((job = await worker.heartbeat().then((worker) => this.assignNextJob(worker, 0, options)))) {
        await job.perform(worker);
      }
    } finally {
      await worker.unregister();
    }
  }

  registerTask(task: Task<Args> | string, taskFn?: TaskHandlerFunction<Args>): void {
    if (typeof task === 'string') {
      const taskName = task;
      const t = new (class implements Task<Args> {
        name = taskName;
        handle = taskFn!;
      })();
      this.taskManager.registerTask(t);
    } else {
      this.taskManager.registerTask(task);
    }
  }

  getNewWorker(options?: WorkerOptions): Worker {
    const config = <WorkerConfig>{
      ...DefaultWorker.DEFAULT_CONFIG,
      queueNames: this.options.queueNames,
      ...(options?.config ?? {}),
    };
    const metadata = options?.metadata ?? {};
    const commands = options?.commands ?? {};
    return new DefaultWorker(this, this.backend, config, metadata, commands);
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

      const { lostWorkers } = await this.backend.pruneWorkers(options.workerLostTimeout);
      if (this.listenerCount('worker_lost') > 0) {
        for (const lostWorker of lostWorkers) {
          this.emit('worker_lost', { worker: lostWorker });
        }
      }

      const { expiredJobs, expungedJobs, abandonedJobs, unattendedJobs } = await this.backend.pruneJobs<Args>(
        options.jobUnattendedPeriod,
        options.jobExpungePeriod,
        [DefaultWorker.FOREGROUND_QUEUE],
      );
      if (this.listenerCount('job_expired') > 0) {
        for (const job of expiredJobs) {
          this.emit('job_expired', { job });
        }
      }
      if (this.listenerCount('job_expunged') > 0) {
        for (const job of expungedJobs) {
          this.emit('job_expunged', { job });
        }
      }
      if (this.listenerCount('job_abandoned') > 0) {
        for (const job of abandonedJobs) {
          this.emit('job_abandoned', { job });
        }
      }
      if (this.listenerCount('job_unattended') > 0) {
        for (const job of unattendedJobs) {
          this.emit('job_unattended', { job });
        }
      }

      for (const jobDescriptor of abandonedJobs) {
        const job = this.createJobObject(jobDescriptor);
        await job.retryFailed();
      }

      this.lastPruneAt = Date.now();
      return lostWorkers.length > 0 || expiredJobs.length > 0;
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
        resolve(info.result);
      } else if (
        info.state === JobState.Failed ||
        info.state === JobState.Aborted ||
        info.state === JobState.Abandoned ||
        info.state === JobState.Unattended ||
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
