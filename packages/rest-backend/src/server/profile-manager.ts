import { type Backend, ConfigurationError, type Job, type JobArgs, type Queue } from '@queuebone/core';
import { type RestWorkerProfileConfig } from './config.js';
import { WorkerProfile } from './profile.js';
import { type ProfileManager, type WorkerProfileId } from './types.js';

const APIKEY_MIN_LENGTH = 64;
const APIKEY_ID_LENGTH = 8;

export class DefaultProfileManager<BaseJob extends Job<JobArgs>>
  extends Map<WorkerProfileId, WorkerProfile<BaseJob>>
  implements ProfileManager<BaseJob>
{
  public static readonly DEFAULT_CONFIG = {
    maxWorkers: Number.MAX_SAFE_INTEGER,
  };

  constructor(
    protected queue: Queue<BaseJob>,
    protected backend: Backend,
    profiles: RestWorkerProfileConfig[] = [],
  ) {
    super();
    profiles.forEach((profile) => this.addProfile(profile));
  }

  timingSafeGet(apikey: string): WorkerProfile<BaseJob> | undefined {
    const id = apikey.substring(0, APIKEY_ID_LENGTH);
    const profile = this.get(id);
    if (profile === undefined) {
      return undefined;
    }
    if (!profile.timingSafeApikeyCompare(apikey)) {
      return undefined;
    }
    return profile;
  }

  addProfile(profileConfig: RestWorkerProfileConfig): WorkerProfileId {
    if (profileConfig.apikey.length < APIKEY_MIN_LENGTH) {
      throw new ConfigurationError(`API key must be at least ${APIKEY_MIN_LENGTH} characters long`);
    }

    const id = profileConfig.apikey.substring(0, APIKEY_ID_LENGTH);
    if (this.has(id)) {
      throw new ConfigurationError(`API key of profile ${profileConfig.name} overlaps with other profile`);
    }

    const profile = new WorkerProfile<BaseJob>(id, profileConfig, this.queue, this.backend);
    this.set(id, profile);

    return id;
  }
}
