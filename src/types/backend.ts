import EventEmitter from 'events';
import {
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  type JobRetryOptions,
  JobState,
  type ListJobsOptions,
} from './job.js';
import {
  type ListWorkersOptions,
  type WorkerCommandArg,
  type WorkerCommandDescriptor,
  type WorkerConfig,
  type WorkerId,
  type WorkerInfo,
  WorkerState,
} from './worker.js';

export type JobInfoList<A extends JobArgs> = {
  jobs: JobInfo<A>[];
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

export interface JobDequeueOptions {
  id?: JobId;
  queueNames: string[];
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

export type JobPruneResult = {
  /**
   * Jobs pending beyond their `expireAt` time, so they are no longer needed. Have been deleted.
   */
  expiredJobs: JobDescriptor[];
  /**
   * Jobs finished as `succeeded` but are beyond expunge period. Have been deleted.
   */
  expungedJobs: JobDescriptor[];
  /**
   * Jobs that have been picked up by a worker, but the worker faded away. Can be rescheduled.
   */
  abandonedJobs: JobDescriptor[];
  /**
   * Jobs that are overdue but haven't been picked up for a given time. Can be rescheduled.
   */
  unattendedJobs: JobDescriptor[];
};

export type WorkerPruneResult = {
  lostWorkers: WorkerInfo[];
};

export interface Backend extends QueueBackend, JobBackend, WorkerBackend, EventEmitter {
  name: string;

  /**
   * Prune workers without heartbeat after the given timeout
   */
  pruneWorkers(listTimeout: number): Promise<WorkerPruneResult>;

  /**
   * Broadcast remote control command to one or more workers.
   */
  sendWorkerCommand(command: string, arg: WorkerCommandArg, options: ListWorkersOptions): Promise<boolean>;

  /**
   * Get statistics for the job queue.
   */
  getStats(): Promise<any>;

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
  addJob(taskName: string, args: JobArgs, options: JobEnqueueOptions): Promise<JobId>;

  /**
   * Looks for a new job in the queues. If a job is found, dequeue it and transition from `pending` to `running`
   * state. Return `null` if queues were empty.
   */
  assignNextJob(
    id: WorkerId,
    taskNames: string[],
    timeout: number,
    options: JobDequeueOptions,
  ): Promise<JobDescriptor | null>;

  /**
   * Prune jobs:
   * 1. Delete jobs that are past expiration time.
   * 2. Expunge successfully finished jobs after `expungePeriod`.
   * 3. Mark `running` jobs of `lost` workers as `abandoned`.
   * 4. Mark `pending` jobs that are overdue as `unattended`.
   */
  pruneJobs(unattendedPeriod: number, expungePeriod: number, excludeQueues: string[]): Promise<JobPruneResult>;

  /**
   * Get history information for job queue.
   */
  getJobHistory(): Promise<any>;

  getJobInfos<A extends JobArgs>(offset: number, limit: number, options: ListJobsOptions): Promise<JobInfoList<A>>;
}

/**
 * The backend methods a `Job` object needs
 */
export interface JobBackend {
  /**
   * Change one or more metadata fields for a job. Setting a value to `null` will remove the field.
   */
  amendJobMetadata(jobId: JobId, records: Record<string, any>): Promise<boolean>;

  /**
   * Updates the job's progress.
   */
  updateJobProgress(id: JobId, attempt: number, progress: number): Promise<boolean>;

  /**
   * Transition from `running` to `succeeded` or `failed` state with or without a result. If the job has failed and
   * if there are attempts remaining, transition back to `pending` with a delay.
   */
  markJobFinished(
    state: JobState.Succeeded | JobState.Failed | JobState.Aborted,
    jobId: JobId,
    attempt: number,
    result: JobResult,
  ): Promise<boolean>;

  /**
   * Transition job back to `pending` state, already `pending` jobs may also be retried to change options.
   */
  retryJob(jobId: JobId, attempt: number, options: JobRetryOptions): Promise<boolean>;

  /**
   * Cancels a job as long as it hasn't been started.
   */
  cancelJob(id: JobId): Promise<boolean>;

  /**
   * Remove `failed`, `succeeded` or `pending` job from queue.
   */
  removeJob(jobId: JobId): Promise<boolean>;
}

/**
 * The backend methods a `Worker` object needs
 */
export interface WorkerBackend {
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

  /**
   * Returns information about a worker.
   */
  getWorkerInfo(id: WorkerId): Promise<WorkerInfo | undefined>;

  /**
   * Returns information about workers in batches.
   */
  getWorkerInfos(offset: number, limit: number, options: ListWorkersOptions): Promise<WorkerInfoList>;
}
