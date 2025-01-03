import { ConfigurationError, type Job, type JobArgs } from '@queuebone/core';
import { timingSafeCompare } from './compare.js';
import { type WorkerProfile } from './config.js';
import { type ProfileManager, type WorkerProfileHolder, type WorkerProfileId } from './types.js';

const APIKEY_MIN_LENGTH = 64;
const APIKEY_ID_LENGTH = 8;

export class DefaultProfileManager<BaseJob extends Job<JobArgs>>
  extends Map<WorkerProfileId, WorkerProfileHolder<BaseJob>>
  implements ProfileManager<BaseJob>
{
  public static readonly DEFAULT_CONFIG = {
    maxWorkers: Number.MAX_SAFE_INTEGER,
  };

  constructor(profiles: WorkerProfile[] = []) {
    super();
    profiles.forEach((profile) => this.addProfile(profile));
  }

  timingSafeGet(apikey: string): WorkerProfileHolder<BaseJob> | undefined {
    const id = apikey.substring(0, APIKEY_ID_LENGTH);
    const holder = this.get(id);
    if (holder === undefined) {
      return undefined;
    }
    if (!timingSafeCompare(Buffer.from(apikey), Buffer.from(holder.apikey))) {
      return undefined;
    }
    return holder;
  }

  addProfile(profile: WorkerProfile): WorkerProfileId {
    if (profile.apikey.length < APIKEY_MIN_LENGTH) {
      throw new ConfigurationError(`API key must be at least ${APIKEY_MIN_LENGTH} characters long`);
    }

    const id = profile.apikey.substring(0, APIKEY_ID_LENGTH);
    if (this.has(id)) {
      throw new ConfigurationError(`API key of profile ${profile.name} overlaps with other profile`);
    }

    const holder: WorkerProfileHolder<BaseJob> = {
      ...DefaultProfileManager.DEFAULT_CONFIG,
      ...profile,
      config: { ...(profile.config ?? {}) },
      id,
      activeWorkers: new Map(),
    };

    this.set(id, holder);

    return id;
  }
}
