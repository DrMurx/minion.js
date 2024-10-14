import { PgBackend } from './backends/pg/backend.js';
import { createPool } from './backends/pg/factory.js';
import { DefaultJob } from './job.js';
import { DefaultQueue } from './queue.js';
import { type Backend } from './types/backend.js';
import {
  type Job,
  type JobAddOptions,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  JobState,
} from './types/job.js';
import { type Queue, type QueueOptions } from './types/queue.js';
import { type Task } from './types/task.js';
import { type Worker, type WorkerConfig, type WorkerId, type WorkerOptions, WorkerState } from './types/worker.js';
import { version } from './version.js';

export default DefaultQueue;
export {
  createPool,
  DefaultJob,
  DefaultQueue,
  JobState,
  PgBackend,
  version,
  WorkerState,
  type Backend,
  type Job,
  type JobAddOptions,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  type Queue,
  type QueueOptions,
  type Task,
  type Worker,
  type WorkerConfig,
  type WorkerId,
  type WorkerOptions,
};
