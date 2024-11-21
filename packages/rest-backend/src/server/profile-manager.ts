import { DefaultWorker, type Job, type JobArgs, type WorkerId } from '@queuebone/core';
import { timingSafeCompare } from './compare.ts';
import { type WorkerProfile } from './config.ts';
import { type ProfileManager, type WorkerProfileHolder } from './types.ts';

export class DefaultProfileManager<BaseJob extends Job<JobArgs>> extends Map implements ProfileManager<BaseJob> {
  private keyBuffers: Buffer[] = [];

  constructor(
    profiles: WorkerProfile[] = [],
    private queueNames: string[] = [],
  ) {
    super();
    profiles.forEach((profile) => this.addProfile(profile));
  }

  clear() {
    super.clear();
    this.keyBuffers = [];
  }

  delete(token: string): boolean {
    const result = super.delete(token);
    const tokenBuffer = Buffer.from(token);
    this.keyBuffers = this.keyBuffers.filter((keyBuffer) => keyBuffer.equals(tokenBuffer));
    return result;
  }

  set(token: string, holder: WorkerProfileHolder<BaseJob>): this {
    super.set(token, holder);
    const tokenBuffer = Buffer.from(token);
    this.keyBuffers.push(tokenBuffer);
    return this;
  }

  timingSafeGet(token: string): WorkerProfileHolder<BaseJob> | undefined {
    if (this.timingSafeHas(token)) {
      return this.get(token);
    }
    return undefined;
  }

  timingSafeHas(token: string): boolean {
    const tokenBuffer = Buffer.from(token);
    const index = this.keyBuffers.findIndex((keyBuffer) => timingSafeCompare(keyBuffer, tokenBuffer));
    return index !== -1;
  }

  addProfile(profile: WorkerProfile): void {
    const token = profile.token;
    if (this.has(token)) {
      throw new Error(`Token ${token} already exists`);
    }

    const holder: WorkerProfileHolder<BaseJob> = {
      ...profile,
      config: {
        ...DefaultWorker.DEFAULT_CONFIG,
        queueNames: this.queueNames,
        ...(profile.config ?? {}),
      },
      maxWorkers: profile.maxWorkers ?? 1,
      activeWorkers: new Map(),
    };

    this.set(token, holder);
  }

  dropWorker(workerId: WorkerId): void {
    this.forEach((holder) => {
      holder.activeWorkers.delete(workerId);
    });
  }
}
