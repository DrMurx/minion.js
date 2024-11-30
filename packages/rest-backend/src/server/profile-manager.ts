import { type Job, type JobArgs } from '@queuebone/core';
import { timingSafeCompare } from './compare.js';
import { type WorkerProfile } from './config.js';
import { type ProfileManager, type WorkerProfileHolder } from './types.js';

export class DefaultProfileManager<BaseJob extends Job<JobArgs>>
  extends Map<string, WorkerProfileHolder<BaseJob>>
  implements ProfileManager<BaseJob>
{
  public static readonly DEFAULT_CONFIG = {
    maxWorkers: Number.MAX_SAFE_INTEGER,
  };

  constructor(profiles: WorkerProfile[] = []) {
    super();
    profiles.forEach((profile) => this.addProfile(profile));
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

  addProfile(profile: WorkerProfile): void {
    const name = profile.name;
    if (this.has(name)) {
      throw new Error(`Profile ${name} already exists`);
    }

    const holder: WorkerProfileHolder<BaseJob> = {
      ...DefaultProfileManager.DEFAULT_CONFIG,
      ...profile,
      config: { ...(profile.config ?? {}) },
      activeWorkers: new Map(),
    };

    this.set(name, holder);
  }
}
