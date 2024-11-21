import { type Job, type JobArgs, type QueueOptions, type WorkerConfig } from '@queuebone/core';

export interface ServerQueueOptions<BaseJob extends Job<JobArgs>> extends QueueOptions<BaseJob> {
  workerProfiles?: WorkerProfile[];
}

export interface WorkerProfile {
  /**
   * Name of this worker profile.
   */
  name: string;

  /**
   * The Bearer Auth token to identify requests for Workers of this profile
   */
  token: string;

  /**
   * Number of workers allowed to connect at the same time with this profile.
   */
  maxWorkers?: number;

  /**
   * The worker config used for this profile
   */
  config?: Partial<WorkerConfig>;
}
