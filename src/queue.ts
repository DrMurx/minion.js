import EventEmitter from 'events';
import { BackendIterator } from './backends/iterator.js';
import { DefaultJob } from './job.js';
import { QueuePruner } from './queue/pruner.js';
import { DefaultTaskManager } from './task-manager.js';
import { type Backend, type JobDequeueOptions, type JobEnqueueOptions } from './types/backend.js';
import {
  type InferJobArgs,
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
  type RunningJob,
  unsuccessfulJobStates,
} from './types/job.js';
import { type PruneOptions, type Queue, QueueEvents, type QueueOptions, type QueueStats } from './types/queue.js';
import { isTask, type Task, type TaskHandlerFunction, type TaskManager } from './types/task.js';
import {
  type ListWorkersOptions,
  type Worker,
  type WorkerCommandArg,
  type WorkerConfig,
  type WorkerInfo,
  type WorkerOptions,
  WorkerState,
} from './types/worker.js';
import { version } from './version.js';
import { DefaultWorker } from './worker.js';

/**
 * Job queue class.
 */
export class DefaultQueue<BaseJob extends Job<JobArgs> = Job<JobArgs>>
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

  private options: QueueOptions;

  protected taskManager: TaskManager<RunningJob<InferJobArgs<BaseJob>>> = new DefaultTaskManager<
    RunningJob<InferJobArgs<BaseJob>>
  >();
  protected pruner: QueuePruner;

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
    this.pruner = new QueuePruner(this, this, this.backend, this.options.pruneInterval, this.options);
  }

  async start(): Promise<void> {
    await this.backend.updateSchema();
    this.pruner.start();
  }

  async stop(): Promise<void> {
    await this.pruner.stop();
    await this.backend.end();
  }

  async addJob<AddedJob extends BaseJob = BaseJob>(
    taskName: string,
    args?: InferJobArgs<BaseJob>,
    options?: JobAddOptions,
  ): Promise<AddedJob> {
    const _args = args ?? ({} as InferJobArgs<BaseJob>);
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
    const jobInfo = <JobInfo<InferJobArgs<BaseJob>>>{
      id,
      taskName,
      args: _args,
      maxAttempts: options?.maxAttempts ?? 1,
      attempt: 1,
    };
    return this.createJobObject<AddedJob>(jobInfo);
  }

  async addJobWithAck<Args extends InferJobArgs<BaseJob>>(
    taskName: string,
    args?: Args,
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

  async getJob<ResultJob extends BaseJob = BaseJob>(id: JobId): Promise<ResultJob | null> {
    const info = await this.backend.getJobInfo<InferJobArgs<ResultJob>>(id);
    if (info === undefined) return null;
    return this.createJobObject<ResultJob>(info);
  }

  async getJobs<ResultJob extends BaseJob = BaseJob>(options: ListJobsOptions): Promise<ResultJob[]> {
    const jobs: ResultJob[] = [];
    for await (const jobInfo of this.listJobInfos<InferJobArgs<ResultJob>>(options)) {
      jobs.push(this.createJobObject<ResultJob>(jobInfo));
    }
    return jobs;
  }

  createJobObject<ResultJob extends BaseJob = BaseJob>(
    jobInfo: JobDescriptor<InferJobArgs<ResultJob>> | JobInfo<InferJobArgs<ResultJob>>,
  ): ResultJob {
    return new DefaultJob<InferJobArgs<ResultJob>>(this.backend, jobInfo) as unknown as ResultJob;
  }

  listJobInfos<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    options: ListJobsOptions = {},
    chunkSize: number = 10,
  ): BackendIterator<JobInfo<Args>> {
    return new BackendIterator<JobInfo<Args>>('jobs', this.backend, options, { chunkSize });
  }

  async getJobStatistics(): Promise<QueueJobStatistics> {
    return await this.backend.getJobHistory();
  }

  async runJob(jobId: number): Promise<boolean> {
    const queueName = this.backend.FOREGROUND_QUEUE;
    const jobCtrl = await this.getJob(jobId);
    if (jobCtrl === null) return false;
    if ((await jobCtrl.retry({ queueName, maxAttempts: jobCtrl.maxAttempts + 1 })) !== true) return false;

    const worker = await this.getNewWorker({ config: { queueNames: [queueName] } }).register();
    try {
      const job = await worker.assignNextJob(0, { id: jobId });
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
      while (true) {
        await worker.heartbeat();
        const job = await worker.assignNextJob(0, options);
        if (!job) break;
        await job.perform(worker);
      }
    } finally {
      await worker.unregister();
    }
  }

  registerTask(
    task: Task<RunningJob<InferJobArgs<BaseJob>>> | string,
    taskFn?: TaskHandlerFunction<RunningJob<InferJobArgs<BaseJob>>>,
  ): void {
    if (typeof task === 'string' && taskFn !== undefined) {
      const taskName = task;
      const handlerFunction = taskFn;
      const t = new (class implements Task<RunningJob<InferJobArgs<BaseJob>>> {
        name = taskName;
        handle = handlerFunction;
      })();
      this.taskManager.registerTask(t);
    } else if (isTask(task)) {
      this.taskManager.registerTask(task);
    } else {
      throw new Error('Invalid task');
    }
  }

  getNewWorker(options?: WorkerOptions): Worker<BaseJob> {
    const config = <WorkerConfig>{
      ...DefaultWorker.DEFAULT_CONFIG,
      queueNames: this.options.queueNames,
      ...(options?.config ?? {}),
    };
    const metadata = options?.metadata ?? {};
    const attachments = options?.attachments ?? {};
    const commands = options?.commands ?? {};
    return new DefaultWorker(this, this.taskManager, this.backend, config, metadata, attachments, commands);
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

  async getStatistics(): Promise<QueueStats> {
    const stats = await this.backend.getStats();

    stats.queueboneVersion = version;
    stats.backendName = this.backend.name;

    return stats;
  }

  async resetQueue(): Promise<void> {
    await this.backend.reset();
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
      const info = await this.backend.getJobInfo(jobId);
      if (info === undefined) {
        resolve(null);
      } else if (info.state === JobState.Succeeded) {
        resolve(info.result);
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
