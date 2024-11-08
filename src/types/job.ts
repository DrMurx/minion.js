import { type WorkerId } from './worker.js';

/**
 * A limited interface for a running `Job` when it is passed to a `Task` handler
 */
export interface Job<Args extends JobArgs> {
  get id(): JobId;
  get taskName(): string;
  get args(): Args;
  get progress(): number;
  get attempt(): number;
  get state(): JobState;

  get abortSignal(): AbortSignal;

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
   * Return the backoff delay in ms.
   */
  getBackoffDelay(): Promise<number>;
}

export type JobId = number;
export type JobArgs = Record<string, any> & { [Symbol.iterator]?: never };
export type JobResult = Record<string, any>;
export type JobError = Record<string, any> | Error;

export type InferJobArgs<J extends Job<JobArgs>> = J extends Job<infer A> ? A : never;

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
  Unattended = 'unattended',
  /**
   * The job was canceled by the user while it was still pending. It may be requeued.
   */
  Canceled = 'canceled',
}

export const unsuccessfulJobStates = [
  JobState.Failed,
  JobState.Aborted,
  JobState.Abandoned,
  JobState.Unattended,
  JobState.Canceled,
];

export interface JobDescriptor<Args extends JobArgs = JobArgs> {
  id: JobId;

  taskName: string;
  args: Args;

  maxAttempts: number;
  attempt: number;
}

export interface JobInfo<Args extends JobArgs = JobArgs> {
  id: JobId;

  queueName: string;
  taskName: string;
  args: Args;
  result?: JobResult;

  state: JobState;
  priority: number;
  progress: number;
  maxAttempts: number;
  attempt: number;

  parentJobIds: JobId[];
  childJobIds: JobId[];
  laxDependency: boolean;

  workerId?: WorkerId;
  metadata: Record<string, any>;

  delayUntil: Date;
  startedAt?: Date;
  retriedAt?: Date;
  finishedAt?: Date;

  createdAt: Date;
  expiresAt?: Date;

  time: Date;
}

export interface ListJobsOptions {
  ids?: JobId[];
  afterId?: number;
  queueNames?: string[];
  taskNames?: string[];
  states?: JobState[];
  metadata?: string[];
}

export interface JobResultOptions {
  interval?: number;
  signal?: AbortSignal;
}
