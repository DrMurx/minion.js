import { JobArgs, type Job, type JobResult } from './job.js';

export type TaskHandlerFunction<A extends JobArgs = JobArgs> = (job: Job<A>) => Promise<JobResult | void>;

export interface Task<A extends JobArgs = JobArgs> {
  readonly name: string;
  handle(job: Job<A>): Promise<JobResult | void>;
}

export interface TaskManager extends TaskReader {
  /**
   * Registers a new task handler.
   */
  registerTask(task: Task): void;
}

export interface TaskReader {
  /**
   * Retrieves a list of all task names.
   */
  getTaskNames(): string[];

  /**
   * Retrieve a task handler.
   * @throws When task unknown
   */
  getTask(taskName: string): Task;
}
