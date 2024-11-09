import EventEmitter from 'events';
import { BackendIterator } from '../backends/iterator.js';
import { type Backend, type JobDequeueOptions, type JobEnqueueOptions, type JobOptions } from '../types/backend.js';
import { JobHandle } from '../types/job-handle.js';
import {
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobId,
  type JobInfo,
  type JobResult,
  type JobResultOptions,
  JobState,
  type ListJobsOptions,
  unsuccessfulJobStates,
} from '../types/job.js';
import { type QueueJobStatistics, type QueueStats } from '../types/queue-stats.js';
import { type PruneOptions, type Queue, QueueEvents, type QueueOptions } from '../types/queue.js';
import { isTask, type Task, type TaskHandlerFunction, type TaskManager } from '../types/task.js';
import {
  type ListWorkersOptions,
  type WorkerCommandArg,
  type WorkerId,
  type WorkerInfo,
  type WorkerInstance,
  type WorkerOptions,
  WorkerState,
} from '../types/worker.js';
import { version } from '../version.js';
import { Executor } from '../worker/executor.js';
import { DefaultJob } from '../worker/job.js';
import { DefaultWorker } from '../worker/worker.js';
import { DefaultJobHandle } from './job-handle.js';
import { QueuePruner } from './pruner.js';
import { DefaultTaskManager } from './task-manager.js';

/**
 * Job queue class.
 */
export class DefaultQueue<BaseJob extends Job<JobArgs> = DefaultJob<JobArgs>>
  extends EventEmitter<QueueEvents<BaseJob>>
  implements Queue<BaseJob>
{
  public static readonly DEFAULT_OPTIONS = Object.freeze(<QueueOptions>{
    queueNames: Object.freeze(['default']),
    pruneInterval: 5 * 60 * 1000,
    workerLostTimeout: 30 * 60 * 1000,
    jobExpungePeriod: 2 * 24 * 60 * 60 * 1000,
    jobUnattendedPeriod: 2 * 24 * 60 * 60 * 1000,
  });

  protected _options: Readonly<QueueOptions>;
  protected taskManager: TaskManager<BaseJob>;
  protected pruner: QueuePruner<BaseJob>;

  /**
   * Constructor
   */
  constructor(
    protected backend: Backend,
    options: Partial<QueueOptions> = {},
  ) {
    super();

    // Assemble and freeze options
    const _options: QueueOptions = { ...DefaultQueue.DEFAULT_OPTIONS, ...options };
    delete _options.tasks;
    if (!Array.isArray(_options.queueNames) || _options.queueNames.length === 0) {
      throw new Error('No queue names given');
    }
    Object.freeze(_options.queueNames);
    this._options = Object.freeze(_options);

    // Create other objects
    this.taskManager = new DefaultTaskManager<BaseJob>(options.tasks);
    this.pruner = new QueuePruner(this.backend, this._options, this);
  }

  get options(): Readonly<QueueOptions> {
    return this._options;
  }

  async start(): Promise<void> {
    await this.backend.updateSchema();
    this.pruner.start();
  }

  async stop(): Promise<void> {
    await this.pruner.stop();
    await this.backend.end();
  }

  async addJob<Args extends InferJobArgs<BaseJob>>(
    taskName: string,
    args?: Args,
    options?: JobOptions,
  ): Promise<JobHandle<Args>> {
    const _args = args ?? ({} as Args);
    const _options = <JobEnqueueOptions>{
      queueName: this._options.queueNames[0],
      priority: 0,
      maxAttempts: 1,
      parentJobIds: [],
      laxDependency: false,
      metadata: {},
      delayFor: 0,
      ...options,
    };
    const jobInfo = await this.backend.addJob(taskName, _args, _options);
    return new DefaultJobHandle(this.backend, jobInfo);
  }

  async addJobWithAck<Result extends JobResult = JobResult, Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    taskName: string,
    args?: Args,
    enqueueOptions?: JobOptions,
    resultOptions?: JobResultOptions,
  ): Promise<Result | null> {
    const job = await this.addJob(taskName, args, enqueueOptions);
    return await this.getJobResult<Result>(job.id, resultOptions ?? {});
  }

  async getJobResult<Result extends JobResult = JobResult>(
    id: JobId,
    options: JobResultOptions = {},
  ): Promise<Result | null> {
    const interval = options.interval ?? 3000;
    const signal = options.signal ?? null;
    return new Promise<Result | null>((resolve, reject) => this.waitForResult(id, interval, signal, resolve, reject));
  }

  async getJob<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(id: JobId): Promise<JobHandle<Args> | null> {
    const jobInfo = await this.backend.getJobInfo<Args>(id);
    if (jobInfo === undefined) return null;
    return new DefaultJobHandle(this.backend, jobInfo);
  }

  async getJobs<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    options: ListJobsOptions,
  ): Promise<JobHandle<Args>[]> {
    const jobs: JobHandle<Args>[] = [];
    for await (const jobInfo of this.listJobInfos<Args>(options)) {
      jobs.push(new DefaultJobHandle(this.backend, jobInfo));
    }
    return jobs;
  }

  createJobObject<ResultJob extends BaseJob>(executor: Executor<ResultJob>): ResultJob {
    return new DefaultJob(executor) as unknown as ResultJob;
  }

  listJobInfos<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    options: ListJobsOptions = {},
    chunkSize: number = 10,
  ): BackendIterator<JobInfo<Args>> {
    return new BackendIterator<JobInfo<Args>>('jobs', this.backend, options, { chunkSize });
  }

  async retryFailedJob(job: BaseJob): Promise<void> {
    if (job.state === JobState.Failed && job.attempt < job.maxAttempts) {
      const options = {
        // Set maxAttempt to its current value (otherwise, `Backend.retryJob` increases it)
        maxAttempts: job.maxAttempts,
        delayFor: await job.getBackoffDelay(),
      };
      await this.backend.retryJob(job.id, job.attempt, options);
    }
  }

  async retryAbandonedJob(jobInfo: JobInfo<InferJobArgs<BaseJob>>): Promise<void> {
    if (jobInfo.state === JobState.Abandoned && jobInfo.attempt < jobInfo.maxAttempts) {
      const options = {
        // Set maxAttempt to its current value (otherwise, `Backend.retryJob` increases it)
        maxAttempts: jobInfo.maxAttempts,
      };
      await this.backend.retryJob(jobInfo.id, jobInfo.attempt, options);
    }
  }

  async runJob(jobId: number): Promise<boolean> {
    const queueName = this.backend.FOREGROUND_QUEUE;
    const jobCtrl = await this.getJob(jobId);
    if (jobCtrl === null) return false;
    if ((await jobCtrl.retry({ queueName, maxAttempts: jobCtrl.maxAttempts + 1 })) === null) return false;

    const worker = await this.getNewWorker({ queueNames: [queueName] }).register();
    try {
      const executor = await worker.getNextExecutor(0, { id: jobId });
      if (executor === null) return false;
      await executor.perform(true);
      return true;
    } finally {
      await worker.unregister();
    }
  }

  async runJobs(options?: JobDequeueOptions): Promise<void> {
    const worker = await this.getNewWorker().register();
    try {
      while (true) {
        await worker.heartbeat();
        const executor = await worker.getNextExecutor(0, options);
        if (executor === null) break;
        await executor.perform();
      }
    } finally {
      await worker.unregister();
    }
  }

  registerTask(task: Task<BaseJob> | string, taskFn?: TaskHandlerFunction<BaseJob>): void {
    if (typeof task === 'string' && taskFn !== undefined) {
      this.taskManager.registerTaskFunction(task, taskFn);
    } else if (isTask(task)) {
      this.taskManager.registerTask(task);
    } else {
      throw new Error('Invalid task');
    }
  }

  getNewWorker(options: Partial<WorkerOptions> = {}): WorkerInstance<BaseJob> {
    const _options = <WorkerOptions>{
      ...DefaultWorker.DEFAULT_CONFIG,
      queueNames: this._options.queueNames,
      ...options,
    };
    return new DefaultWorker(this.backend, _options, this.taskManager, this, this.backend, this);
  }

  async getWorkerInfo(worker: WorkerId | WorkerInstance<BaseJob>): Promise<WorkerInfo | undefined> {
    const id = typeof worker === 'number' ? worker : worker.id;
    if (id === undefined) return undefined;
    return await this.backend.getWorkerInfo(id);
  }

  listWorkerInfos(options: ListWorkersOptions = {}, chunkSize: number = 10): BackendIterator<WorkerInfo> {
    const _options: ListWorkersOptions = {
      state: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
      ...options,
    };
    return new BackendIterator<WorkerInfo>('workers', this.backend, _options, { chunkSize });
  }

  async sendWorkerCommand(command: string, arg: WorkerCommandArg, options?: ListWorkersOptions): Promise<boolean> {
    const _options: ListWorkersOptions = {
      state: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
      ...(options ?? {}),
    };
    return await this.backend.sendWorkerCommand(command, arg ?? {}, _options);
  }

  async prune(extraOptions: Partial<PruneOptions> = {}): Promise<boolean> {
    return await this.pruner.perform(true, extraOptions);
  }

  async getJobStatistics(): Promise<QueueJobStatistics> {
    return await this.backend.getJobHistory();
  }

  async getStatistics(): Promise<QueueStats> {
    const stats = await this.backend.getStats();

    stats.queueboneVersion = version;
    stats.backendName = this.backend.name;

    return stats;
  }

  async resetQueue(): Promise<void> {
    await this.backend.reset();
  }

  protected async waitForResult<Result extends JobResult>(
    jobId: JobId,
    interval: number,
    signal: AbortSignal | null,
    resolve: (value: Result | null) => void,
    reject: (reason?: any) => void,
  ) {
    const rerun = () => this.waitForResult(jobId, interval, signal, resolve, reject);
    try {
      const info = await this.backend.getJobInfo(jobId);
      if (info === undefined) {
        resolve(null);
      } else if (info.state === JobState.Succeeded) {
        resolve(info.result as Result);
      } else if (unsuccessfulJobStates.includes(info.state)) {
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
