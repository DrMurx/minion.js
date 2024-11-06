import { type JobOptions } from './backend.js';
import { type JobArgs, type JobId, type JobResult, JobState } from './job.js';
import { type WorkerId } from './worker.js';

/**
 * A limited interface for a running `Job` when it is passed to a `Task` handler
 */
export interface JobHandle<Args extends JobArgs> {
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
   * Time of the last update
   */
  get time(): Date;

  /**
   * The children job ids of this job
   */
  getChildJobIds(): Promise<JobId[]>;

  /**
   * Transition job back to `pending` or `scheduled` state. Already `pending` jobs may also be retried to change options.
   * If successful, it will return a new `JobHandle` object reflecting the changes.
   */
  retry(options?: JobOptions): Promise<JobHandle<Args> | null>;

  /**
   * Change one or more metadata fields for this job. Setting a value to `null` will remove the field. Only values
   * that can be serialized as JSON are supported.
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
