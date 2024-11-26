import { DefaultWorker, type Job, type JobArgs, type WorkerId } from '@queuebone/core';
import { timingSafeCompare } from './compare.js';
import { type WorkerProfile } from './config.js';
import { type ProfileManager, type WorkerProfileHolder } from './types.js';

export class DefaultProfileManager<BaseJob extends Job<JobArgs>> extends Map implements ProfileManager<BaseJob> {
  public static readonly DEFAULT_CONFIG = {
    maxWorkers: Number.MAX_SAFE_INTEGER,
  };

  constructor(
    profiles: WorkerProfile[] = [],
    private defaultQueueNames: string[] = [],
  ) {
    super();
    profiles.forEach((profile) => this.addProfile(profile));
  }

  clear() {
    super.clear();
  }

  delete(name: string): boolean {
    const result = super.delete(name);
    return result;
  }

  set(name: string, holder: WorkerProfileHolder<BaseJob>): this {
    super.set(name, holder);
    return this;
  }

  timingSafeGet(name: string, passphrase: string): WorkerProfileHolder<BaseJob> | undefined {
    const holder = this.get(name);
    if (holder === undefined) {
      return undefined;
    }
    if (timingSafeCompare(Buffer.from(passphrase), Buffer.from(holder.passphrase))) {
      return holder;
    } else {
      return undefined;
    }
  }

  timingSafeHas(name: string, passphrase: string): boolean {
    const holder = this.get(name);
    if (holder === undefined) {
      return false;
    }
    return timingSafeCompare(Buffer.from(passphrase), Buffer.from(holder.passphrase));
  }

  addProfile(profile: WorkerProfile): void {
    const name = profile.name;
    if (this.has(name)) {
      throw new Error(`Profile ${name} already exists`);
    }

    const holder: WorkerProfileHolder<BaseJob> = {
      ...DefaultProfileManager.DEFAULT_CONFIG,
      ...profile,
      config: {
        ...DefaultWorker.DEFAULT_CONFIG,
        queueNames: this.defaultQueueNames,
        ...(profile.config ?? {}),
      },
      activeWorkers: new Map(),
    };

    this.set(name, holder);
  }

  dropWorker(workerId: WorkerId): void {
    this.forEach((holder) => {
      holder.activeWorkers.delete(workerId);
    });
  }
}
