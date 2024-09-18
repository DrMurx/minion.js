import { type Task, type TaskManager } from './types/task.js';

export class DefaultTaskManager implements TaskManager {
  private tasks: TaskList = {};

  registerTask(taskName: string, fn: Task): void {
    this.tasks[taskName] = fn;
  }

  getTaskNames(): string[] {
    return Object.keys(this.tasks);
  }

  getTask(taskName: string): Task {
    if (!this.tasks[taskName]) throw new Error(`Unknown task ${taskName}`);
    return this.tasks[taskName];
  }
}

type TaskList = Record<string, Task>;
