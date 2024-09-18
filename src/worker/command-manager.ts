import {
  type Worker,
  type WorkerCommandArg,
  type WorkerCommandDescriptor,
  type WorkerCommandHandler,
} from '../types/worker.js';

export class WorkerCommandManager {
  /**
   * Registered commands.
   */
  private commands: Map<string, WorkerCommandHandler> = new Map();

  constructor(
    private worker: Worker,
    commands: Record<string, WorkerCommandHandler>,
  ) {
    this.addDefaultHandlers();
    for (const [name, fn] of Object.entries(commands)) {
      this.addHandler(name, fn);
    }
  }

  addHandler(commandName: string, fn: WorkerCommandHandler): void {
    this.commands.set(commandName, fn);
  }

  async runCommands(commands: WorkerCommandDescriptor[]) {
    for (const { command, arg } of commands) {
      await this.runCommand(command, arg);
    }
  }

  private async runCommand(commandName: string, arg: WorkerCommandArg) {
    const handlerFn = this.commands.get(commandName);
    if (handlerFn === undefined) return;

    try {
      await handlerFn(this.worker, arg);
    } catch (e) {
      console.error(`Command ${commandName}@${this.worker.id} failed: ${e}`);
    }
  }

  private addDefaultHandlers() {
    this.addHandler('setConcurrency', async (worker, { concurrency }) => {
      // Remote control commands need to validate arguments carefully
      const jobs = parseInt(concurrency);
      if (isNaN(jobs) === false) await worker.setConfig({ concurrency: jobs });
    });

    this.addHandler('terminate', async (worker) => {
      await worker.stop();
    });
  }
}
