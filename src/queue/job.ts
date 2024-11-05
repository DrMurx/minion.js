import { JobResult, JobState, type Job, type JobArgs, type JobId } from '../types/job.js';
import { type RunningWorker } from '../types/worker.js';
import { type Executor } from '../worker/executor.js';

/**
 * Default job class.
 */
export class DefaultJob<Args extends JobArgs> implements Job<Args> {
  public worker: RunningWorker<Job<Args>> | null = null;

  constructor(private executor: Executor<Job<Args>>) {}

  get id(): JobId {
    return this.executor.id;
  }

  get taskName(): string {
    return this.executor.taskName;
  }

  get args(): Args {
    return this.executor.args;
  }

  get result(): JobResult | undefined {
    return this.executor.result;
  }

  get progress(): number {
    return this.executor.progress;
  }

  get attempt(): number {
    return this.executor.attempt;
  }

  get state(): JobState {
    return this.executor.state;
  }

  get abortSignal(): AbortSignal {
    return this.executor.abortSignal;
  }

  async updateProgress(progress: number): Promise<boolean> {
    return this.executor.updateProgress(progress);
  }

  async amendMetadata(records: Record<string, any>): Promise<boolean> {
    return await this.executor.amendMetadata(records);
  }

  async getBackoffDelay(): Promise<number> {
    return this.attempt ** 4 + 15;
  }
}
