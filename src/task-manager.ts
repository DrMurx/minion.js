import { RunningJob, type JobArgs } from './types/job.js';
import { type Task, type TaskManager } from './types/task.js';

export class DefaultTaskManager<TaskJob extends RunningJob<JobArgs>> implements TaskManager<TaskJob> {
  private tasks: TaskList<TaskJob> = new Map();

  registerTask(task: Task<TaskJob>): void {
    this.tasks.set(task.name, task);
  }

  getTaskNames(): string[] {
    return Array.from(this.tasks.keys());
  }

  getTask(taskName: string): Task<TaskJob> {
    if (!this.tasks.has(taskName)) throw new Error(`Unknown task ${taskName}`);
    return this.tasks.get(taskName)!;
  }
}

type TaskList<TaskJob extends RunningJob<JobArgs>> = Map<string, Task<TaskJob>>;
