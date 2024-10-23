import { type JobArgs, type JobResult, type RunningJob } from './job.js';
import { type RunningWorker } from './worker.js';

export type TaskHandlerFunction<TaskJob extends RunningJob<JobArgs> = RunningJob<JobArgs>> = (
  job: TaskJob,
  worker: RunningWorker,
) => Promise<JobResult | void>;

export interface Task<TaskJob extends RunningJob<JobArgs> = RunningJob<JobArgs>> {
  readonly name: string;
  handle(job: TaskJob, worker: RunningWorker): Promise<JobResult | void>;
}

export function isTask(t: any): t is Task {
  return typeof t.name === 'string' && typeof t.handle === 'function';
}

export interface TaskManager<TaskJob extends RunningJob<JobArgs>> {
  /**
   * Registers a new task handler.
   */
  registerTask(task: Task<TaskJob>): void;

  /**
   * Retrieve a task handler.
   * @throws When task unknown
   */
  getTask(taskName: string): Task<TaskJob>;

  /**
   * Retrieves a list of all task names.
   */
  getTaskNames(): string[];
}
