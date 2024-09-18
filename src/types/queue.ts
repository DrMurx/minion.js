import { type BackendIterator } from '../backends/iterator.js';
import {
  type Job,
  type JobArgs,
  type JobDequeueOptions,
  type JobEnqueueOptions,
  type JobId,
  type JobInfo,
  type JobResultOptions,
  type ListJobsOptions,
  type QueueJobStatistics,
} from './job.js';
import { type Task } from './task.js';
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
export interface Queue extends JobManager, JobExecutor, WorkerManager, StatsReader {
  /**
   * Enqueue a new job with `pending` or `scheduled` state. Arguments can only be simple scalars, maps or arrays.
   * @param options.queueName     - Queue to put job in, defaults to `default`.
   * @param options.priority      - Job priority, defaults to `0`. Jobs with a higher priority get performed first.
   *                                Priorities can be positive or negative.
   * @param options.maxAttempts   - Number of times performing this job will be attempted, with a delay based on the
   *                                backoff function after the first attempt, defaults to `1`.
   * @param options.metadata      - Object with arbitrary metadata for this job that gets serialized as JSON.
   * @param options.parentJobIds  - One or more existing jobs this job depends on, and that need to have transitioned
   *                                to a finished state before it can be processed.
   * @param options.laxDependency - If `false`, parent jobs must be `successful`, if `true`, any completion will do.
   * @param options.delayUntil    - Delay job for this many milliseconds (from now), defaults to `0`.
   * @param options.expireAt      - Job is valid for this many milliseconds (from now) before it expires.
   */
  addJob(taskName: string, args?: JobArgs, options?: JobEnqueueOptions): Promise<JobId>;

  /**
   * Register a task.
   */
  registerTask(taskName: string, fn: Task): void;

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
   * Ensure that backend schema is updated to the latest version.
   */
  updateSchema(): Promise<void>;

  /**
   * Reset job queue.
   */
  resetQueue(): Promise<void>;

  /**
   * Stop using the queue.
   */
  end(): Promise<void>;
}

export interface JobManager {
  /**
   * Retrieve a Job object (without making any changes to the actual job), or return `null` if job does not exist.
   */
  getJob(id: JobId): Promise<Job | null>;

  /**
   * Get an array ob Job objects according to the specified options.
   */
  getJobs(options: ListJobsOptions): Promise<Job[]>;

  /**
   * Return a promise for the future result of a job. The state `succeeded` will result in the promise being
   * `fullfilled`, and the state `failed` in the promise being `rejected`.
   */
  getJobResult(jobId: JobId, options: JobResultOptions): Promise<JobInfo | null>;

  /**
   * Get job data or return `null` if job does not exist.
   */
  getJobInfo(jobId: JobId): Promise<JobInfo | undefined>;

  /**
   * Return iterator object to safely iterate through job information as returned by the backend.
   */
  listJobInfos(options?: ListJobsOptions, chunkSize?: number): BackendIterator<JobInfo>;
}

export interface JobExecutor {
  /**
   * Retry job in a foreground queue, then perform it right away with a temporary worker in this process,
   * very useful for debugging.
   */
  runJob(id: number): Promise<boolean>;

  /**
   * Perform all jobs with a temporary worker, very useful for testing.
   */
  runJobs(options?: JobDequeueOptions): Promise<void>;
}

export interface QueueReader {
  /**
   * Wait a given amount of time in milliseconds for a job, dequeue job object and transition from `pending` to
   * `running` state for the given worker, or return `null` if queues were empty.
   */
  assignNextJob(worker: Worker, wait?: number, options?: JobDequeueOptions): Promise<Job | null>;
}

export interface WorkerManager {
  /**
   * Build worker object.
   */
  getNewWorker(options?: WorkerOptions): Worker;

  /**
   * Return iterator object to safely iterate through worker information.
   */
  listWorkerInfos(options?: ListWorkersOptions, chunkSize?: number): BackendIterator<WorkerInfo>;
}

export interface StatsReader {
  /**
   * Get history information for job queue.
   */
  getJobStatistics(): Promise<QueueJobStatistics>;

  /**
   * Get statistics for the job queue.
   */
  getStatistics(): Promise<QueueStats>;
}

export interface QueueOptions extends PruneOptions {
  pruneInterval: number;
}

export interface PruneOptions {
  /**
   * Amount of time in milliseconds after which workers without contact will be considered missing and removed from
   * the registry. Defaults to 30 minutes.
   */
  workerMissingTimeout: number;

  /**
   * Amount of time in milliseconds after which jobs that have reached the state `succeeded` and have no unresolved
   * dependencies will be removed automatically from the queue. Defaults to 2 days. It is not recommended to set this
   * value any lower.
   */
  jobRetentionPeriod: number;

  /**
   * Amount of time in milliseconds after which jobs that have not been processed will be considered stuck by
   * and transition to the `failed` state. Defaults to 2 days.
   */
  jobStuckTimeout: number;
}

export interface QueueStats {
  enqueuedJobs: number;
  pendingJobs: number;
  scheduledJobs: number;
  runningJobs: number;
  succeededJobs: number;
  failedJobs: number;
  abortedJobs: number;
  abandonedJobs: number;
  stuckJobs: number;
  canceledJobs: number;

  offlineWorkers: number;
  onlineWorkers: number;
  idleWorkers: number;
  busyWorkers: number;
  missingWorkers: number;

  queueboneVersion: string;

  backendName: string;
  backendVersion: string;
  backendUptime: number;
}
