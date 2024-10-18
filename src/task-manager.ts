import { type JobArgs } from './types/job.js';
import { type Task, type TaskManager } from './types/task.js';

export class DefaultTaskManager<A extends JobArgs> implements TaskManager<A> {
  private tasks: TaskList<A> = new Map();

  registerTask(task: Task<A>): void {
    this.tasks.set(task.name, task);
  }

  getTaskNames(): string[] {
    return Array.from(this.tasks.keys());
  }

  getTask(taskName: string): Task<A> {
    if (!this.tasks.has(taskName)) throw new Error(`Unknown task ${taskName}`);
    return this.tasks.get(taskName)!;
  }
}

type TaskList<A extends JobArgs> = Map<string, Task<A>>;
