import { type Job, type JobResult } from './job.js';

export type Task = (job: Job) => Promise<JobResult>;

export interface TaskManager extends TaskReader {
  /**
   * Registers a new task handler.
   */
  registerTask(taskName: string, fn: Task): void;
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
