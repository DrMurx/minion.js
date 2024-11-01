import { type Job, type JobArgs } from './types/job.js';
import { type Task, type TaskManager } from './types/task.js';

export class DefaultTaskManager<BaseJob extends Job<JobArgs>> implements TaskManager<BaseJob> {
  private tasks: TaskList<BaseJob> = new Map();

  registerTask(task: Task<BaseJob>): void {
    this.tasks.set(task.name, task);
  }

  getTaskNames(): string[] {
    return Array.from(this.tasks.keys());
  }

  getTask(taskName: string): Task<BaseJob> {
    if (!this.tasks.has(taskName)) {
      throw new Error(`Unknown task ${taskName}`);
    }
    return this.tasks.get(taskName)!;
  }
}

type TaskList<TaskJob extends Job<JobArgs>> = Map<string, Task<TaskJob>>;
