import { type JobArgs, type JobResult, type RunningJob } from './job.js';

export type TaskHandlerFunction<A extends JobArgs> = (job: RunningJob<A>) => Promise<JobResult | void>;

export interface Task<A extends JobArgs = JobArgs> {
  readonly name: string;
  handle(job: RunningJob<A>): Promise<JobResult | void>;
}

export interface TaskReader<A extends JobArgs> {
  /**
   * Retrieves a list of all task names.
   */
  getTaskNames(): string[];

  /**
   * Retrieve a task handler.
   * @throws When task unknown
   */
  getTask(taskName: string): Task<A>;
}

export interface TaskManager<A extends JobArgs> extends TaskReader<A> {
  /**
   * Registers a new task handler.
   */
  registerTask(task: Task<A>): void;
}
