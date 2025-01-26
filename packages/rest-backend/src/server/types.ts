import { type Job, type JobArgs } from '@queuebone/core';
import { type WorkerProfile } from './profile.js';

export type WorkerProfileId = string;

export interface ProfileManager<BaseJob extends Job<JobArgs>>
  extends ReadonlyMap<WorkerProfileId, WorkerProfile<BaseJob>> {
  timingSafeGet(apikey: string): WorkerProfile<BaseJob> | undefined;
}

export interface RemoteWorkerMetadata {
  ip: string;
  hostname: string | undefined;
  pid: number | undefined;
}
