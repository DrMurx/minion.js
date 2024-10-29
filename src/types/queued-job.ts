import { JobOptions } from './backend.js';
import { type JobArgs, type JobId, type JobResult, JobState } from './job.js';
import { type WorkerId } from './worker.js';

/**
 * A limited interface for a running `Job` when it is passed to a `Task` handler
 */
export interface QueuedJob<Args extends JobArgs> {
  get id(): JobId;

  get queueName(): string;
  get taskName(): string;
  get args(): Args;
  get result(): JobResult;

  get state(): JobState;
  get priority(): number;
  get progress(): number;
  get maxAttempts(): number;
  get attempt(): number;

  get parentJobIds(): JobId[];
  get laxDependency(): boolean;

  get workerId(): WorkerId | undefined;
  get metadata(): Record<string, any>;

  get delayUntil(): Date;
  get startedAt(): Date | undefined;
  get retriedAt(): Date | undefined;
  get finishedAt(): Date | undefined;

  get createdAt(): Date;
  get expiresAt(): Date | undefined;

  /**
   * Transition job back to `pending` or `scheduled` state. Already `pending` jobs may also be retried to change options.
   * If successful, it will return a new `QueueJob` object.
   */
  retry(options?: JobOptions): Promise<QueuedJob<Args> | null>;

  /**
   * Change one or more metadata fields for this job. Setting a value to `null` will remove the field. The new values
   * will get serialized as JSON.
   */
  amendMetadata(records: Record<string, any>): Promise<boolean>;

  /**
   * Cancel job as long as it hasn't been started.
   */
  cancel(): Promise<boolean>;

  /**
   * Remove job from queue (unless it's `running`).
   */
  remove(): Promise<boolean>;

  /**
   * Sync job information from the database
   */
  sync(): Promise<boolean>;
}
