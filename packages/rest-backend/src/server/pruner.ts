import { type Job, type JobArgs, type PruneOptions } from '@queuebone/core';
import { type ProfileManager } from './types.js';

export class Pruner<BaseJob extends Job<JobArgs>> {
  private pruneScheduler: NodeJS.Timeout | undefined;

  constructor(
    protected profileManager: ProfileManager<BaseJob>,
    protected options: PruneOptions,
  ) {}

  startPruner(): void {
    this.prune();
    if (!this.pruneScheduler) {
      this.pruneScheduler = setInterval(() => this.prune(), this.options.pruneInterval);
    }
  }

  stopPruner(): void {
    if (this.pruneScheduler) {
      clearInterval(this.pruneScheduler);
      this.pruneScheduler = undefined;
    }
  }

  prune(): void {
    const expireAfter = Date.now() - this.options.workerLostTimeout;
    this.profileManager.forEach((profile) => {
      for (const [workerId, worker] of profile.activeWorkers) {
        if (worker.isExpired(expireAfter)) {
          profile.activeWorkers.delete(workerId);
        } else {
          worker.pruneExecutorProxies(expireAfter);
        }
      }
    });
  }
}
