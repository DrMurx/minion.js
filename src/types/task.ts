import { type JobArgs, type JobResult, type RunningJob } from './job.js';
import { type RunningWorker } from './worker.js';

export type TaskHandlerFunction<BaseJob extends RunningJob<JobArgs> = RunningJob<JobArgs>> = (
  job: BaseJob,
  worker: RunningWorker<BaseJob>,
) => Promise<JobResult | void>;

export interface Task<BaseJob extends RunningJob<JobArgs> = RunningJob<JobArgs>> {
  readonly name: string;
  handle(job: BaseJob, worker: RunningWorker<BaseJob>): Promise<JobResult | void>;
}

export function isTask(t: any): t is Task {
  return typeof t.name === 'string' && typeof t.handle === 'function';
}

export interface TaskManager<BaseJob extends RunningJob<JobArgs>> {
  /**
   * Registers a new task handler.
   */
  registerTask(task: Task<BaseJob>): void;

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
