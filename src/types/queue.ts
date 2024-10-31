import type EventEmitter from 'events';
import { type BackendIterator } from '../backends/iterator.js';
import { type JobDequeueOptions, type JobOptions } from './backend.js';
import {
  JobError,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  type JobResultOptions,
  type ListJobsOptions,
  type RunningJob,
} from './job.js';
import { type StatsReader } from './queue-stats.js';
import { type QueuedJob } from './queued-job.js';
import { type Task, type TaskHandlerFunction } from './task.js';
import {
  type ListWorkersOptions,
  type Worker,
  type WorkerCommandArg,
  type WorkerInfo,
  type WorkerOptions,
} from './worker.js';

/**
 * The public queue interface
 */
export interface Queue<BaseJob extends Job<JobArgs> = Job<JobArgs>>
  extends JobFactory<BaseJob>,
    QuickWorker,
    WorkerManager<BaseJob>,
    StatsReader,
    EventEmitter<QueueEvents<BaseJob>> {
  /**
   * Starts the queue and ensure that backend schema is updated to the latest version.
   */
  start(): Promise<void>;

  /**
   * Release all resources and stop using the queue.
   */
  stop(): Promise<void>;

  /**
   * Enqueue a new job with `pending` or `scheduled` state. Arguments can only be simple scalars, maps or arrays.
   * @param options.queueName     - Queue to put job in, defaults to the first queueName given to the `Queue`.
   * @param options.priority      - Job priority, defaults to `0`. Jobs with a higher priority get performed first.
   *                                Priorities can be positive or negative.
   * @param options.maxAttempts   - Number of times performing this job will be attempted, with a delay based on the
   *                                backoff function after the first attempt, defaults to `1`.
   * @param options.metadata      - Object with arbitrary metadata for this job that gets serialized as JSON.
   * @param options.parentJobIds  - One or more existing jobs this job depends on, and that need to have transitioned
   *                                to a finished state before it can be processed.
   * @param options.laxDependency - If `false`, parent jobs must be `successful`, if `true`, any completion will do.
   * @param options.delayFor      - Delay job for this many milliseconds (from now), defaults to `0`. If given, the job
   *                                will start in the `scheduled` state.
   * @param options.expireIn      - Job becomes invalid/expired after this many milliseconds (from now). If given, the
   *                                job will be deleted once it's expired.
   */
  addJob<Args extends InferJobArgs<BaseJob>>(
    taskName: string,
    args?: Args,
    options?: JobOptions,
  ): Promise<QueuedJob<Args>>;

  addJobWithAck<Args extends InferJobArgs<BaseJob>>(
    taskName: string,
    args?: Args,
    enqueueOptions?: JobOptions,
    resultOptions?: JobResultOptions,
  ): Promise<JobResult>;

  /**
   * Return a promise for the future result of a job. The state `succeeded` will result in the promise being
   * `fullfilled`, and the state `failed` in the promise being `rejected`.
   */
  getJobResult(jobId: JobId, options: JobResultOptions): Promise<JobResult>;

  /**
   * Retrieve a Job control object, or return `null` if job does not exist.
   */
  getJob<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(id: JobId): Promise<QueuedJob<Args> | null>;

  /**
   * Get an array of Job control objects according to the specified options.
   */
  getJobs<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    options: ListJobsOptions,
  ): Promise<QueuedJob<Args>[]>;

  /**
   * Return iterator object to safely iterate through job information as returned by the backend.
   */
  listJobInfos<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    options?: ListJobsOptions,
    chunkSize?: number,
  ): BackendIterator<JobInfo<Args>>;

  /**
   * Register a task.
   */
  registerTask(task: Task<RunningJob<InferJobArgs<BaseJob>>>): void;
  registerTask(taskName: string, fn: TaskHandlerFunction<RunningJob<InferJobArgs<BaseJob>>>): void;

  /**
   * Broadcast remote control command to one or more workers. Unless `option.state` is specified, commands
   * will only be sent to online workers (idle/busy).
   */
  sendWorkerCommand(command: string, arg?: WorkerCommandArg, options?: ListWorkersOptions): Promise<boolean>;

  /**
   * Force a prune run on the worker registry and job queue outside the regular schedule. This action will restart the timer
   * for the next prune.
   */
  prune(extraOptions?: Partial<PruneOptions>): Promise<boolean>;

  /**
   * Reset job queue.
   */
  resetQueue(): Promise<void>;
}

export interface JobFactory<BaseJob extends Job<JobArgs>> {
  createJobObject<ResultJob extends BaseJob = BaseJob>(
    jobInfo: JobDescriptor<InferJobArgs<ResultJob>> | JobInfo<InferJobArgs<ResultJob>>,
  ): ResultJob;
}

export interface WorkerManager<BaseJob extends Job<JobArgs>> {
  /**
   * Build worker object.
   */
  getNewWorker(options?: WorkerOptions): Worker<BaseJob>;

  /**
   * Return iterator object to safely iterate through worker information.
   */
  listWorkerInfos(options?: ListWorkersOptions, chunkSize?: number): BackendIterator<WorkerInfo>;
}

export interface QuickWorker {
  /**
   * Retry job in a foreground queue, then perform it right away with a temporary worker in this process,
   * very useful for debugging.
   */
  runJob(id: number): Promise<boolean>;

  /**
   * Perform all jobs with a temporary worker, very useful for testing.
   */
  runJobs(options?: Partial<JobDequeueOptions>): Promise<void>;
}

export interface PruneOptions {
  /**
   * Amount of time in milliseconds after which workers without contact will be considered `lost` and marked as
   * such.
   */
  workerLostTimeout: number;

  /**
   * Amount of time in milliseconds after which jobs that have reached the state `succeeded` and have no unresolved
   * dependencies will be removed automatically from the queue.
   */
  jobExpungePeriod: number;

  /**
   * Amount of time in milliseconds after which jobs that have not been processed will transition to the
   * `unattended` state.
   */
  jobUnattendedPeriod: number;
}

export interface QueueOptions extends PruneOptions {
  /**
   * Names of the queues
   */
  queueNames: string[];
  pruneInterval: number;
  jobFactory: JobFactoryFunction;
}

export type JobFactoryFunction<BaseJob extends Job<JobArgs> = Job<JobArgs>> = (
  jobInfo: JobDescriptor<InferJobArgs<BaseJob>>,
) => BaseJob;

export interface QueueEvents<BaseJob extends Job<JobArgs>> {
  job_started: [{ job: JobDescriptor<InferJobArgs<BaseJob>> }];
  job_succeeded: [{ job: JobDescriptor<InferJobArgs<BaseJob>>; result: JobResult }];
  job_failed: [{ job: JobDescriptor<InferJobArgs<BaseJob>>; result: JobError }];
  job_expired: [{ job: JobDescriptor<InferJobArgs<BaseJob>> }];
  job_expunged: [{ job: JobDescriptor<InferJobArgs<BaseJob>> }];
  job_abandoned: [{ job: JobDescriptor<InferJobArgs<BaseJob>> }];
  job_unattended: [{ job: JobDescriptor<InferJobArgs<BaseJob>> }];
  worker_lost: [{ worker: Worker<BaseJob> }];
}
