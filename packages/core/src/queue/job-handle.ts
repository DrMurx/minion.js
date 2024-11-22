import { type JobHandleBackend, type JobOptions } from '../types/backend.js';
import { type JobHandle } from '../types/job-handle.js';
import { JobState, type JobArgs, type JobId, type JobRecord, type JobResult } from '../types/job.js';
import { type WorkerId } from '../types/worker.js';

/**
 * Job Handle
 */
export class DefaultJobHandle<Args extends JobArgs = JobArgs> implements JobHandle<Args> {
  private jobRecord: JobRecord<Args>;
  private updateTime: Date;

  constructor(
    private backend: JobHandleBackend,
    jobRecord: JobRecord<Args>,
  ) {
    this.jobRecord = { ...jobRecord };
    this.updateTime = new Date();
  }

  get id(): JobId {
    return this.jobRecord.id;
  }

  get queueName(): string {
    return this.jobRecord.queueName;
  }

  get taskName(): string {
    return this.jobRecord.taskName;
  }

  get args(): Args {
    return this.jobRecord.args;
  }

  get result(): JobResult | undefined {
    return this.jobRecord.result;
  }

  get state(): JobState {
    return this.jobRecord.state;
  }

  get priority(): number {
    return this.jobRecord.priority;
  }

  get progress(): number {
    return this.jobRecord.progress;
  }

  get maxAttempts(): number {
    return this.jobRecord.maxAttempts;
  }

  get attempt(): number {
    return this.jobRecord.attempt;
  }

  get parentJobIds(): JobId[] {
    return this.jobRecord.parentJobIds;
  }

  get laxDependency(): boolean {
    return this.jobRecord.laxDependency;
  }

  get workerId(): WorkerId | undefined {
    return this.jobRecord.workerId;
  }

  get metadata(): Record<string, any> {
    return this.jobRecord.metadata;
  }

  get delayUntil(): Date {
    return this.jobRecord.delayUntil;
  }

  get startedAt(): Date | undefined {
    return this.jobRecord.startedAt;
  }

  get retriedAt(): Date | undefined {
    return this.jobRecord.retriedAt;
  }

  get finishedAt(): Date | undefined {
    return this.jobRecord.finishedAt;
  }

  get createdAt(): Date {
    return this.jobRecord.createdAt;
  }

  get expiresAt(): Date | undefined {
    return this.jobRecord.expiresAt;
  }

  get time(): Date {
    return this.updateTime;
  }

  async getChildJobIds(): Promise<JobId[]> {
    const jobInfo = await this.backend.getJobInfo<Args>(this.id);
    if (jobInfo !== undefined) {
      return jobInfo.childJobIds;
    }
    return [];
  }

  async retry(options: JobOptions = {}): Promise<JobHandle<Args> | null> {
    const jobRecord = await this.backend.retryJob<Args>(this.id, this.attempt, options);
    if (jobRecord !== undefined) {
      return new DefaultJobHandle(this.backend, jobRecord);
    }
    return null;
  }

  async amendMetadata(records: Record<string, any>): Promise<boolean> {
    const metadata = await this.backend.amendJobMetadata(this.id, this.attempt, records);
    const isUpdated = metadata !== undefined;
    if (isUpdated) {
      this.jobRecord.metadata = metadata;
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
      this.jobRecord = jobInfo;
      this.updateTime = jobInfo.time;
    }
    return jobInfo !== undefined;
  }
}
