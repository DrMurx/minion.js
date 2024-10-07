import { type Task, type TaskManager } from './types/task.js';

export class DefaultTaskManager implements TaskManager {
  private tasks: TaskList = new Map();

  registerTask(task: Task): void {
    this.tasks.set(task.name, task);
  }

  getTaskNames(): string[] {
    return Array.from(this.tasks.keys());
  }

  getTask(taskName: string): Task {
    if (!this.tasks.has(taskName)) throw new Error(`Unknown task ${taskName}`);
    return this.tasks.get(taskName)!;
  }
}

type TaskList = Map<string, Task>;
