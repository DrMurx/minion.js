import { ConfigurationError, InvalidStateError } from '../errors.js';
import { type Job, type JobArgs } from '../types/job.js';
import { type Task, type TaskHandlerFunction, type TaskManager } from '../types/task.js';

export class DefaultTaskManager<BaseJob extends Job<JobArgs>> implements TaskManager<BaseJob> {
  private tasks: TaskList<BaseJob> = new Map();

  constructor(tasks?: Task<BaseJob>[] | { [taskName: string]: TaskHandlerFunction<BaseJob> }) {
    if (tasks) {
      if (Array.isArray(tasks)) {
        this.registerTasks(tasks);
      } else if (typeof tasks === 'object') {
        this.registerTaskFunctions(tasks);
      } else {
        throw new ConfigurationError('Invalid tasks given');
      }
    }
  }

  registerTask(task: Task<BaseJob>): void {
    this.tasks.set(task.name, task);
  }

  registerTaskFunction(taskName: string, taskFn: TaskHandlerFunction<BaseJob>): void {
    const t = new (class implements Task<BaseJob> {
      name = taskName;
      handle = taskFn;
    })();
    this.registerTask(t);
  }

  registerTasks(tasks: Task<BaseJob>[]): void {
    for (const task of tasks) {
      this.registerTask(task);
    }
  }

  registerTaskFunctions(tasks: { [taskName: string]: TaskHandlerFunction<BaseJob> }): void {
    for (const taskName in tasks) {
      this.registerTaskFunction(taskName, tasks[taskName]);
    }
  }

  getTaskNames(): string[] {
    return Array.from(this.tasks.keys());
  }

  getTask(taskName: string): Task<BaseJob> {
    if (!this.tasks.has(taskName)) {
      throw new InvalidStateError(`Unknown task ${taskName}`);
    }
    return this.tasks.get(taskName)!;
  }
}

type TaskList<TaskJob extends Job<JobArgs>> = Map<string, Task<TaskJob>>;
