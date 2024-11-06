import { type JobHandleBackend, type JobOptions } from '../types/backend.js';
import { type JobHandle } from '../types/job-handle.js';
import { JobState, type JobArgs, type JobId, type JobInfo, type JobResult } from '../types/job.js';
import { type WorkerId } from '../types/worker.js';

/**
 * Job Handle
 */
export class DefaultJobHandle<Args extends JobArgs = JobArgs> implements JobHandle<Args> {
  private jobInfo: JobInfo<Args>;

  constructor(
    private backend: JobHandleBackend,
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

  get time(): Date {
    return this.jobInfo.time;
  }

  async getChildJobIds(): Promise<JobId[]> {
    // Todo: remove childJobIds from regular getJobInfo and move it into dedicated backend method
    await this.sync();
    return this.jobInfo.childJobIds;
  }

  async retry(options: JobOptions = {}): Promise<JobHandle<Args> | null> {
    const jobInfo = await this.backend.retryJob<Args>(this.id, this.attempt, options);
    if (jobInfo !== undefined) {
      return new DefaultJobHandle(this.backend, jobInfo);
    }
    return null;
  }

  async amendMetadata(records: Record<string, any>): Promise<boolean> {
    const metadata = await this.backend.amendJobMetadata(this.id, this.attempt, records);
    const isUpdated = metadata !== undefined;
    if (isUpdated) {
      this.jobInfo.metadata = metadata;
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
    if (jobInfo !== undefined) {
      this.jobInfo = jobInfo;
    }
    return jobInfo !== undefined;
  }
}
