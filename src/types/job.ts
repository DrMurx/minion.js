import { type Worker, type WorkerId } from './worker.js';

export interface Job<A extends JobArgs> {
  get id(): JobId;
  get taskName(): string;
  get args(): A;
  get state(): JobState | undefined;
  get progress(): number;
  get maxAttempts(): number;
  get attempt(): number;
  get abortSignal(): AbortSignal;

  /**
   * Perform job and wait for it to finish. Note that this method should only be used to implement custom workers.
   */
  perform(worker: Worker, throwOnError?: boolean): Promise<void>;

  /**
   * Update job progress.
   */
  updateProgress(progress: number): Promise<boolean>;

  /**
   * Change one or more metadata fields for this job. Setting a value to `null` will remove the field. The new values
   * will get serialized as JSON.
   */
  amendMetadata(records: Record<string, any>): Promise<boolean>;

  /**
   * Transition from `running` to `succeeded` state with or without a result.
   */
  markSucceeded(result?: JobResult): Promise<boolean>;

  /**
   * Transition from `running` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `pending` with a delay based on the backoff policy.
   */
  markFailed(result?: JobResult | Error): Promise<boolean>;

  /**
   * Transition job back to `pending` or `scheduled` state. Already `pending` jobs may also be retried to change options.
   */
  retry(options?: JobRetryOptions): Promise<boolean>;

  /**
   * Retry a failed job if there are still attempts left.
   */
  retryFailed(): Promise<boolean>;

  /**
   * Cancel job as long as it hasn't been started.
   */
  cancel(): Promise<boolean>;

  /**
   * Remove `failed`, `succeeded` or `pending` job from queue.
   */
  remove(): Promise<boolean>;

  /**
   * Get job information.
   */
  getInfo(): Promise<JobInfo | undefined>;

  /**
   * Return all jobs this job depends on.
   */
  getParentJobs(): Promise<Job<JobArgs>[]>;

  /**
   * Return the backoff delay in ms.
   */
  getBackoffDelay(): Promise<number>;
}

export type JobId = number;

export type JobArgs = Record<string, any> & { [Symbol.iterator]?: never };

export type JobResult = Record<string, any>;
export type JobError = Record<string, any> | Error;

export enum JobState {
  /**
   * The job is pending for immediate execution.
   */
  Pending = 'pending',
  /**
   * The job is pending, but scheduled for execution after a specified time.
   */
  Scheduled = 'scheduled',
  /**
   * The job is currently executed.
   */
  Running = 'running',
  /**
   * The job has finished sucessfully.
   */
  Succeeded = 'succeeded',
  /**
   * The job has finished with a failure. It will be requeued.
   */
  Failed = 'failed',
  /**
   * The job was picked up by a worker, but the worker terminated gracefully. It will be requeued.
   */
  Aborted = 'aborted',
  /**
   * The job was picked up by a worker, but the worker fainted. It will be requeued.
   */
  Abandoned = 'abandoned',
  /**
   * The job was pending for too long and may require manual intervention. It may be requeued.
   */
  Stuck = 'stuck',
  /**
   * The job was canceled by the user while it was still pending. It will be requeued.
   */
  Canceled = 'canceled',
}

export interface ListJobsOptions {
  ids?: JobId[];
  afterId?: number;
  queueNames?: string[];
  taskNames?: string[];
  states?: JobState[];
  metadata?: string[];
}

export interface JobDescriptor<A extends JobArgs = JobArgs> {
  id: JobId;

  taskName: string;
  args: A;

  maxAttempts: number;
  attempt: number;
}

export interface JobInfo<A extends JobArgs = JobArgs> extends JobDescriptor<A> {
  id: JobId;

  queueName: string;
  taskName: string;
  args: A;
  result: JobResult;

  state: JobState;
  priority: number;
  progress: number;
  maxAttempts: number;
  attempt: number;

  parentJobIds: JobId[];
  childJobIds: JobId[];
  laxDependency: boolean;

  workerId: WorkerId;
  metadata: Record<string, any>;

  delayUntil: Date;
  startedAt: Date;
  retriedAt: Date;
  finishedAt: Date;

  createdAt: Date;
  expiresAt: Date;

  time: Date;
}

export interface JobDequeueOptions {
  id?: JobId;
  queueNames?: string[];
  minPriority?: number;
}

export interface JobEnqueueOptions {
  queueName?: string;

  priority?: number;
  maxAttempts?: number;

  metadata?: Record<string, any>;

  parentJobIds?: JobId[];
  laxDependency?: boolean;

  delayFor?: number;
  expireIn?: number;
}

export type JobRetryOptions = Omit<JobEnqueueOptions, 'metadata'>;

export interface JobResultOptions {
  interval?: number;
  signal?: AbortSignal;
}

export interface QueueJobStatistics {
  daily: DailyJobHistory[];
}

export interface DailyJobHistory {
  epoch: number;
  succeededJobs: number;
  failedJobs: number;
  abortedJobs: number;
  abandonedJobs: number;
  stuckJobs: number;
}
