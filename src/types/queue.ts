import type EventEmitter from 'events';
import { type BackendIterator } from '../backends/iterator.js';
import { type JobOptions } from './backend.js';
import { type JobHandle } from './job-handle.js';
import {
  JobState,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobBackoffStrategy,
  type JobDescriptor,
  type JobError,
  type JobFactory,
  type JobId,
  type JobInfo,
  type JobResult,
  type JobResultOptions,
  type ListJobsOptions,
} from './job.js';
import { type StatsReader } from './queue-stats.js';
import { type Task, type TaskHandlerFunction } from './task.js';
import {
  type ListWorkersOptions,
  type WorkerCommandArg,
  type WorkerId,
  type WorkerInfo,
  type WorkerInstance,
  type WorkerOptions,
} from './worker.js';

/**
 * The public queue interface, aggregating Producer, Consumer, Controller and some other interfaces
 */
export interface Queue<BaseJob extends Job<JobArgs> = Job<JobArgs>>
  extends Producer<BaseJob>,
    Consumer<BaseJob>,
    Controller,
    StatsReader,
    QueueEventEmitter<BaseJob> {
  /**
   * Name of the foreground queue
   */
  readonly FOREGROUND_QUEUE: string;

  /**
   * Access to the queue options
   */
  get options(): Readonly<QueueOptions<BaseJob>>;

  /**
   * Starts the queue and ensure that backend schema is updated to the latest version.
   */
  start(): Promise<void>;

  /**
   * Release all resources and stop using the queue.
   */
  stop(): Promise<void>;
}

export interface Producer<BaseJob extends Job<JobArgs>> {
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
  ): Promise<JobHandle<Args>>;

  addJobWithAck<Result extends JobResult = JobResult, Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    taskName: string,
    args?: Args,
    enqueueOptions?: JobOptions,
    resultOptions?: JobResultOptions,
  ): Promise<Result | null>;

  /**
   * Return a promise for the future result of a job. The state `succeeded` will result in the promise being
   * `fullfilled`, and the state `failed` in the promise being `rejected`.
   */
  getJobResult<Result extends JobResult = JobResult>(jobId: JobId, options: JobResultOptions): Promise<Result | null>;

  /**
   * Retrieve a Job control object, or return `null` if job does not exist.
   */
  getJob<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(id: JobId): Promise<JobHandle<Args> | null>;

  /**
   * Get an array of Job control objects according to the specified options.
   */
  getJobs<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    options: ListJobsOptions,
  ): Promise<JobHandle<Args>[]>;

  /**
   * Return iterator object to safely iterate through job information as returned by the backend.
   */
  listJobInfos<Args extends InferJobArgs<BaseJob> = InferJobArgs<BaseJob>>(
    options?: ListJobsOptions,
    chunkSize?: number,
  ): BackendIterator<JobInfo<Args>>;
}

export interface Consumer<BaseJob extends Job<JobArgs>> {
  /**
   * Register a task.
   */
  registerTask(task: Task<BaseJob>): void;
  registerTask(taskName: string, fn: TaskHandlerFunction<BaseJob>): void;

  /**
   * Build worker object.
   */
  getNewWorker(options?: Partial<WorkerOptions>): WorkerInstance<BaseJob>;

  /**
   * Get worker information.
   */
  getWorkerInfo(worker: WorkerId | WorkerInstance<BaseJob>): Promise<WorkerInfo | undefined>;

  /**
   * Return iterator object to safely iterate through worker information.
   */
  listWorkerInfos(options?: ListWorkersOptions, chunkSize?: number): BackendIterator<WorkerInfo>;
}

export interface Controller {
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

export type QueueEventEmitter<BaseJob extends Job<JobArgs>> = EventEmitter<QueueEvents<BaseJob>>;

// --------------------------------------------------------------

export interface PruneOptions {
  pruneInterval: number;

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

export interface QueueOptions<BaseJob extends Job<JobArgs>> extends PruneOptions {
  /**
   * Names of the queues
   */
  queueNames: string[];

  /**
   * Tasks to register at Queue construction times
   */
  tasks?: Task[] | { [taskName: string]: TaskHandlerFunction };

  /**
   * Factory class to create new jobs
   */
  jobFactory?: JobFactory<BaseJob>;

  /**
   * Function to calculate a backoff strategy
   */
  backoffStrategy?: JobBackoffStrategy<InferJobArgs<BaseJob>>;
}

export interface QueueEvents<
  BaseJob extends Job<JobArgs>,
  JobDescriptorRO = Readonly<JobDescriptor<InferJobArgs<BaseJob>>>,
  JobInfoRO = Readonly<JobInfo<InferJobArgs<BaseJob>>>,
> {
  job_started: [{ job: BaseJob; jobInfo: JobInfoRO }];
  job_progress: [{ job: BaseJob; jobInfo: JobInfoRO; progress: number; duration: number }];
  job_finished: [{ job: BaseJob; jobInfo: JobInfoRO; state: JobState; duration: number }];
  job_succeeded: [{ job: BaseJob; jobInfo: JobInfoRO; result: Readonly<JobResult>; duration: number }];
  job_failed: [{ job: BaseJob; jobInfo: JobInfoRO; result: Readonly<JobError>; duration: number }];
  job_expired: [{ jobInfo: JobDescriptorRO }];
  job_expunged: [{ jobInfo: JobDescriptorRO }];
  job_abandoned: [{ jobInfo: JobInfoRO }];
  job_unattended: [{ jobInfo: JobDescriptorRO }];
  worker_lost: [{ workerInfo: Readonly<WorkerInfo> }];
  prune_run: [{ force: boolean; options: PruneOptions }];
}
