import { type Job, type JobArgs, type JobResult } from './job.js';
import { type RunningWorker } from './worker.js';

export type TaskHandlerFunction<BaseJob extends Job<JobArgs> = Job<JobArgs>> = (
  job: BaseJob,
  worker: RunningWorker<BaseJob>,
) => Promise<JobResult | void>;

export interface Task<BaseJob extends Job<JobArgs> = Job<JobArgs>> {
  readonly name: string;
  handle(job: BaseJob, worker: RunningWorker<BaseJob>): Promise<JobResult | void>;
}

export function isTask(t: any): t is Task {
  return typeof t.name === 'string' && typeof t.handle === 'function';
}

export interface TaskManager<BaseJob extends Job<JobArgs>> {
  /**
   * Registers a new task handler.
   */
  registerTask(task: Task<BaseJob>): void;

  /**
   * Registers a new task handler given as function.
   */
  registerTaskFunction(taskName: string, taskFn: TaskHandlerFunction<BaseJob>): void;

  /**
   * Retrieve a task handler.
   * @throws When task unknown
   */
  getTask(taskName: string): Task<BaseJob>;

  /**
   * Retrieves a list of all task names.
   */
  getTaskNames(): string[];
}
