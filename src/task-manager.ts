import { type JobArgs } from './types/job.js';
import { type Task, type TaskManager } from './types/task.js';

export class DefaultTaskManager<Args extends JobArgs> implements TaskManager<Args> {
  private tasks: TaskList<Args> = new Map();

  registerTask(task: Task<Args>): void {
    this.tasks.set(task.name, task);
  }

  getTaskNames(): string[] {
    return Array.from(this.tasks.keys());
  }

  getTask(taskName: string): Task<Args> {
    if (!this.tasks.has(taskName)) throw new Error(`Unknown task ${taskName}`);
    return this.tasks.get(taskName)!;
  }
}

type TaskList<Args extends JobArgs> = Map<string, Task<Args>>;
