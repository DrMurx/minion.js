import { type Executor } from '../worker/executor.js';
import { type RunningWorker, type WorkerId } from './worker.js';

/**
 * A limited interface for a running `Job` when it is passed to a `Task` handler
 */
export interface Job<Args extends JobArgs> {
  get id(): JobId;
  get taskName(): string;
  get args(): Readonly<Args>;
  get result(): Readonly<JobResult> | undefined;
  get progress(): number;
  get attempt(): number;
  get maxAttempts(): number;
  get state(): JobState;
  get worker(): RunningWorker<Job<Args>>;
  get metadata(): Readonly<Record<string, any>>;

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
}

export type JobId = number;
export type JobArgs = Record<string, any> & { [Symbol.iterator]?: never };
export type JobResult = Record<string, any>;
export type JobError = Record<string, any> | Error;

export type InferJobArgs<J extends Job<JobArgs>> = J extends Job<infer A> ? A : never;

/**
 * Return the backoff delay for a failed job in ms.
 */
export type JobBackoffStrategy<Args extends JobArgs = JobArgs> = (jobRecord: JobRecord<Args>) => number;

export interface JobFactory<BaseJob extends Job<JobArgs>> {
  createJobObject<ResultJob extends BaseJob>(executor: Executor<ResultJob>): ResultJob;
}

export enum JobState {
  /**
   * The job is pending for execution until `delayUntil` has passed.
   */
  Pending = 'pending',
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

export interface JobRecord<Args extends JobArgs = JobArgs> {
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
  laxDependency: boolean;

  workerId?: WorkerId;
  metadata: Record<string, any>;

  delayUntil: Date;
  startedAt?: Date;
  retriedAt?: Date;
  finishedAt?: Date;

  createdAt: Date;
  expiresAt?: Date;
}

export interface JobInfo<Args extends JobArgs = JobArgs> extends JobRecord<Args> {
  childJobIds: JobId[];
  time: Date;
}

export interface ListJobsOptions {
  ids?: JobId[];
  afterId?: number;
  queueNames?: string[];
  taskNames?: string[];
  states?: JobState[];
  metadata?: Record<string, any>[];
}

export interface JobResultOptions {
  interval?: number;
  signal?: AbortSignal;
}
