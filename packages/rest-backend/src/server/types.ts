import { type Job, type JobArgs, type WorkerConfig, type WorkerId } from '@queuebone/core';
import { type WorkerProxy } from './worker-proxy.js';

export type WorkerProfileId = string;

export interface WorkerProfileHolder<BaseJob extends Job<JobArgs>> {
  id: WorkerProfileId;
  name: string;
  apikey: string;
  maxWorkers: number;
  config: Partial<WorkerConfig>;
  activeWorkers: Map<WorkerId, WorkerProxy<BaseJob>>;
}

export interface ProfileManager<BaseJob extends Job<JobArgs>>
  extends ReadonlyMap<WorkerProfileId, WorkerProfileHolder<BaseJob>> {
  timingSafeGet(apikey: string): WorkerProfileHolder<BaseJob> | undefined;
}
