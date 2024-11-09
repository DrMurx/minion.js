import { PgBackend } from './backends/pg/backend.js';
import { createPool } from './backends/pg/factory.js';
import { DefaultQueue } from './queue/queue.js';
import { type Backend, type JobOptions } from './types/backend.js';
import { type JobHandle } from './types/job-handle.js';
import {
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  JobState,
} from './types/job.js';
import { type Queue, type QueueOptions } from './types/queue.js';
import { type Task, type TaskHandlerFunction, type TaskManager } from './types/task.js';
import {
  type RunningWorker,
  type WorkerConfig,
  type WorkerId,
  type WorkerInstance,
  type WorkerOptions,
  WorkerState,
} from './types/worker.js';
import { version } from './version.js';
import { Executor } from './worker/executor.js';
import { DefaultJob } from './worker/job.js';
import { DefaultTaskManager } from './worker/task-manager.js';

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
  type Executor,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDescriptor,
  type JobHandle,
  type JobId,
  type JobInfo,
  type JobOptions,
  type JobResult,
  type Queue,
  type QueueOptions,
  type RunningWorker,
  type Task,
  type TaskHandlerFunction,
  type TaskManager,
  type WorkerConfig,
  type WorkerId,
  type WorkerInstance,
  type WorkerOptions,
};
