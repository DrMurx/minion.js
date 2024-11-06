import { type Job, type JobArgs } from '../types/job.js';
import { type Task } from '../types/task.js';
import { type RunningWorker, type WorkerId } from '../types/worker.js';

/**
 * Represents a lost worker
 */
export class GhostWorker<BaseJob extends Job<JobArgs>> implements RunningWorker<BaseJob> {
  get id(): WorkerId | undefined {
    return 0;
  }
  getMetadata<T = any>(): T | undefined {
    return undefined;
  }
  getAttachment<T = any>(): T {
    return {} as T;
  }
  get abortSignal(): AbortSignal {
    return new AbortController().signal;
  }
  getTask(): Task<BaseJob> {
    return null as unknown as Task<BaseJob>;
  }
  async heartbeat(): Promise<this> {
    return this;
  }
}
