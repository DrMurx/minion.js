import { PgBackend } from './backends/pg/backend.js';
import { createPool } from './backends/pg/factory.js';
import { DefaultJob } from './job.js';
import { DefaultQueue } from './queue.js';
import { DefaultTaskManager } from './task-manager.js';
import { type Backend, type JobOptions } from './types/backend.js';
import {
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  JobState,
  type RunningJob,
} from './types/job.js';
import { type Queue, type QueueOptions } from './types/queue.js';
import { type QueuedJob } from './types/queued-job.js';
import { type Task, type TaskHandlerFunction, type TaskManager } from './types/task.js';
import {
  type RunningWorker,
  type Worker,
  type WorkerConfig,
  type WorkerId,
  type WorkerOptions,
  WorkerState,
} from './types/worker.js';
import { version } from './version.js';

export default DefaultQueue;
export {
  createPool,
  DefaultJob,
  DefaultQueue,
  DefaultTaskManager,
  JobState,
  PgBackend,
  version,
  WorkerState,
  type Backend,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobOptions,
  type JobResult,
  type Queue,
  type QueuedJob,
  type QueueOptions,
  type RunningJob,
  type RunningWorker,
  type Task,
  type TaskHandlerFunction,
  type TaskManager,
  type Worker,
  type WorkerConfig,
  type WorkerId,
  type WorkerOptions,
};
