import { type JobBackend, type JobOptions } from './types/backend.js';
import { JobState, type JobArgs, type JobId, type JobInfo, type JobResult } from './types/job.js';
import { type QueuedJob } from './types/queued-job.js';
import { type WorkerId } from './types/worker.js';

/**
 * Job Controller
 */
export class DefaultQueuedJob<Args extends JobArgs = JobArgs> implements QueuedJob<Args> {
  private jobInfo: JobInfo<Args>;

  constructor(
    private backend: JobBackend,
    jobInfo: JobInfo<Args>,
  ) {
    this.jobInfo = { ...jobInfo };
  }

  get id(): JobId {
    return this.jobInfo.id;
  }

  get queueName(): string {
    return this.jobInfo.queueName;
  }

  get taskName(): string {
    return this.jobInfo.taskName;
  }

  get args(): Args {
    return this.jobInfo.args;
  }

  get result(): JobResult {
    return this.jobInfo.result;
  }

  get state(): JobState {
    return this.jobInfo.state;
  }

  get priority(): number {
    return this.jobInfo.priority;
  }

  get progress(): number {
    return this.jobInfo.progress;
  }

  get maxAttempts(): number {
    return this.jobInfo.maxAttempts;
  }

  get attempt(): number {
    return this.jobInfo.attempt;
  }

  get parentJobIds(): JobId[] {
    return this.jobInfo.parentJobIds;
  }

  get laxDependency(): boolean {
    return this.jobInfo.laxDependency;
  }

  get workerId(): WorkerId | undefined {
    return this.jobInfo.workerId;
  }

  get metadata(): Record<string, any> {
    return this.jobInfo.metadata;
  }

  get delayUntil(): Date {
    return this.jobInfo.delayUntil;
  }

  get startedAt(): Date | undefined {
    return this.jobInfo.startedAt;
  }

  get retriedAt(): Date | undefined {
    return this.jobInfo.retriedAt;
  }

  get finishedAt(): Date | undefined {
    return this.jobInfo.finishedAt;
  }

  get createdAt(): Date {
    return this.jobInfo.createdAt;
  }

  get expiresAt(): Date | undefined {
    return this.jobInfo.expiresAt;
  }

  async retry(options: JobOptions = {}): Promise<QueuedJob<Args> | null> {
    const jobInfo = await this.backend.retryJob<Args>(this.id, this.attempt, options);
    if (jobInfo) {
      return new DefaultQueuedJob(this.backend, jobInfo);
    }
    return null;
  }

  async amendMetadata(records: Record<string, any>): Promise<boolean> {
    const isUpdated = await this.backend.amendJobMetadata(this.id, records);
    if (isUpdated) {
      await this.sync();
    }
    return isUpdated;
  }

  async cancel(): Promise<boolean> {
    const isUpdated = await this.backend.cancelJob(this.id);
    if (isUpdated) {
      await this.sync();
    }
    return isUpdated;
  }

  async remove(): Promise<boolean> {
    return await this.backend.removeJob(this.id);
  }

  async sync(): Promise<boolean> {
    const jobInfo = await this.backend.getJobInfo<Args>(this.id);
    if (jobInfo) {
      this.jobInfo = jobInfo;
    }
    return !!jobInfo;
  }
}
