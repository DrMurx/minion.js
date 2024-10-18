import { type JobArgs, type JobResult, type RunningJob } from './job.js';

export type TaskHandlerFunction<Args extends JobArgs> = (job: RunningJob<Args>) => Promise<JobResult | void>;

export interface Task<Args extends JobArgs = JobArgs> {
  readonly name: string;
  handle(job: RunningJob<Args>): Promise<JobResult | void>;
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
