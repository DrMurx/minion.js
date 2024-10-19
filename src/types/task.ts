import { type JobArgs, type JobResult, type RunningJob } from './job.js';

export type TaskHandlerFunction<Args extends JobArgs, ArgsJob extends RunningJob<Args> = RunningJob<Args>> = (
  job: ArgsJob,
) => Promise<JobResult | void>;

export interface Task<Args extends JobArgs = JobArgs, ArgsJob extends RunningJob<Args> = RunningJob<Args>> {
  readonly name: string;
  handle(job: ArgsJob): Promise<JobResult | void>;
}

export function isTask(t: any): t is Task {
  return typeof t.name === 'string' && typeof t.handle === 'function';
}

export interface TaskReader<Args extends JobArgs> {
  /**
   * Retrieves a list of all task names.
   */
  getTaskNames(): string[];

  /**
   * Retrieve a task handler.
   * @throws When task unknown
   */
  getTask(taskName: string): Task<Args>;
}

export interface TaskManager<Args extends JobArgs> extends TaskReader<Args> {
  /**
   * Registers a new task handler.
   */
  registerTask(task: Task<Args>): void;
}
