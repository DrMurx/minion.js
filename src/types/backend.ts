import type EventEmitter from 'events';
import {
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  JobState,
  type ListJobsOptions,
} from './job.js';
import { type QueueJobStatistics } from './queue-stats.js';
import {
  type ListWorkersOptions,
  type WorkerCommandArg,
  type WorkerCommandDescriptor,
  type WorkerConfig,
  type WorkerId,
  type WorkerInfo,
  WorkerState,
} from './worker.js';

export interface Backend extends QueueBackend, IteratorBackend, JobHandleBackend, WorkerBackend, EventEmitter {
  readonly FOREGROUND_QUEUE: string;
  readonly name: string;

  /**
   * Update storage schemas to latest version.
   */
  updateSchema(): Promise<void>;

  /**
   * Reset job queue.
   */
  reset(): Promise<void>;

  /**
   * Release the backend.
   */
  end(): Promise<void>;
}

/**
 * The backend methods a `Queue` object needs
 */
export interface QueueBackend {
  /**
   * Enqueue a new job with `pending` state.
   */
  addJob<Args extends JobArgs>(taskName: string, args: Args, options: JobEnqueueOptions): Promise<JobInfo<Args>>;

  /**
   * Prune jobs:
   * 1. Delete jobs that are past expiration time.
   * 2. Expunge successfully finished jobs after `expungePeriod`.
   * 3. Mark `running` jobs of `lost` workers as `abandoned`.
   * 4. Mark `pending` jobs that are overdue as `unattended`.
   */
  pruneJobs<Args extends JobArgs>(unattendedPeriod: number, expungePeriod: number): Promise<JobPruneResult<Args>>;

  /**
   * Returns information about a worker.
   */
  getWorkerInfo(id: WorkerId): Promise<WorkerInfo | undefined>;

  /**
   * Prune workers without heartbeat after the given timeout
   */
  pruneWorkers(listTimeout: number): Promise<WorkerPruneResult>;

  /**
   * Broadcast remote control command to one or more workers.
   */
  sendWorkerCommand(command: string, arg: WorkerCommandArg, options: ListWorkersOptions): Promise<boolean>;

  /**
   * Get history information for job queue.
   */
  getJobHistory(): Promise<QueueJobStatistics>;

  /**
   * Get statistics for the job queue.
   */
  getStats(): Promise<any>;
}

export interface IteratorBackend {
  /**
   * Returns the information about jobs in batches.
   */
  getJobInfos<Args extends JobArgs>(
    offset: number,
    limit: number,
    options: ListJobsOptions,
  ): Promise<JobInfoList<Args>>;

  /**
   * Returns information about workers in batches.
   */
  getWorkerInfos(offset: number, limit: number, options: ListWorkersOptions): Promise<WorkerInfoList>;
}

/**
 * The backend methods a `Job` object needs
 */
export interface JobBackend {
  /**
   * Change one or more metadata fields for a job. Setting a value to `null` will remove the field.
   */
  amendJobMetadata(
    jobId: JobId,
    attempt: number,
    records: Record<string, any>,
  ): Promise<Record<string, any> | undefined>;

  /**
   * Updates the job's progress.
   */
  updateJobProgress(jobId: JobId, attempt: number, progress: number): Promise<boolean>;

  /**
   * Transition from `running` to `succeeded` or `failed` state with or without a result. If the job has failed and
   * if there are attempts remaining, transition back to `pending` with a delay.
   */
  markJobFinished(
    jobId: JobId,
    attempt: number,
    state: JobState.Succeeded | JobState.Failed | JobState.Aborted,
    result: JobResult,
  ): Promise<boolean>;

  /**
   * Transition job back to `pending` state, already `pending` jobs may also be retried to change options. Note that
   * this method will always increase the `attempt` field. The `maxAttempts` field will also be increased by default
   * unless `options.maxAttempts` is set.
   */
  retryJob<Args extends JobArgs>(
    jobId: JobId,
    attempt: number,
    options: JobOptions,
  ): Promise<JobInfo<Args> | undefined>;
}

/**
 * The backend methods a `JobHandle` object needs
 */
export interface JobHandleBackend extends JobBackend {
  /**
   * Returns the information about a specific job.
   */
  getJobInfo<Args extends JobArgs>(jobId: JobId): Promise<JobInfo<Args> | undefined>;

  /**
   * Cancels a job as long as it hasn't been started.
   */
  cancelJob(id: JobId): Promise<boolean>;

  /**
   * Remove a job currently not in `running` state from the queue.
   */
  removeJob(jobId: JobId): Promise<boolean>;
}

/**
 * The backend methods a `WorkerInstance` object needs
 */
export interface WorkerBackend {
  /**
   * Looks for a new job in the queues. If a job is found, dequeue it and transition from `pending` to `running`
   * state. Return `null` if queues were empty.
   */
  assignNextJob<Args extends JobArgs>(
    id: WorkerId,
    taskNames: string[],
    timeout: number,
    options: JobDequeueOptions,
  ): Promise<JobInfo<Args> | null>;

  /**
   * Register a new worker.
   */
  registerWorker(options: WorkerRegistrationOptions): Promise<number>;

  /**
   * Update worker's data (including its `lastSeenAt` date).
   */
  updateWorker(id: WorkerId, options: WorkerRegistrationOptions): Promise<boolean>;

  /**
   * Update some of the worker's data (`status`, `finishedJobCount` and `lastSeenAt`), and receive
   * remote control commands.
   */
  checkWorkerInbox(id: WorkerId, options: WorkerInboxOptions): Promise<WorkerCommandDescriptor[]>;

  /**
   * Unregister worker.
   */
  unregisterWorker(id: WorkerId): Promise<boolean>;
}

export type JobInfoList<Args extends JobArgs> = {
  jobs: JobInfo<Args>[];
  total: number;
};

export interface JobEnqueueOptions {
  queueName: string;

  priority: number;
  maxAttempts: number;

  metadata: Record<string, any>;

  parentJobIds: JobId[];
  laxDependency: boolean;

  delayFor: number;
  expireIn?: number;
}

export type JobOptions = Partial<JobEnqueueOptions>;

/**
 * Options used when retrieving a new job for execution
 */
export interface JobDequeueOptions {
  /**
   * Pick this specific job
   */
  id?: JobId;
  /**
   * Select a job from the given queue(s)
   */
  queueNames: string | string[];
  /**
   * Select a job of at least this priority
   */
  minPriority?: number;
}

export type WorkerInfoList = {
  workers: WorkerInfo[];
  total: number;
};

export type WorkerRegistrationOptions = {
  config: WorkerConfig;
  state: WorkerState;
  finishedJobCount: number;
  metadata: Record<string, any>;
};

export type WorkerInboxOptions = {
  state: WorkerState;
  finishedJobCount: number;
};

export type JobPruneResult<Args extends JobArgs = JobArgs> = {
  /**
   * Jobs pending beyond their `expireAt` time, so they are no longer needed. Have been deleted.
   */
  expiredJobs: JobDescriptor<Args>[];
  /**
   * Jobs finished as `succeeded` but are beyond expunge period. Have been deleted.
   */
  expungedJobs: JobInfo<Args>[];
  /**
   * Jobs that have been picked up by a worker, but the worker faded away. Can be rescheduled.
   */
  abandonedJobs: JobInfo<Args>[];
  /**
   * Jobs that are overdue but haven't been picked up for a given time. Can be rescheduled.
   */
  unattendedJobs: JobDescriptor<Args>[];
};

export type WorkerPruneResult = {
  lostWorkers: WorkerInfo[];
};
