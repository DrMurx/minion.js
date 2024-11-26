import { type Job, type JobArgs, type WorkerConfig, type WorkerId } from '@queuebone/core';
import { type WorkerProfile } from './config.js';
import { type WorkerProxy } from './worker-proxy.js';

export interface WorkerProfileHolder<BaseJob extends Job<JobArgs>> {
  name: string;
  passphrase: string;
  maxWorkers: number;
  config: WorkerConfig;
  activeWorkers: Map<WorkerId, WorkerProxy<BaseJob>>;
}

export interface ProfileManager<BaseJob extends Job<JobArgs>>
  extends ReadonlyMap<string, WorkerProfileHolder<BaseJob>> {
  timingSafeGet(name: string, passphrase: string): WorkerProfileHolder<BaseJob> | undefined;
  addProfile(profile: WorkerProfile): void;
  startPruner(): void;
  stopPruner(): void;
}
