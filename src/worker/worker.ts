import {
  type JobBackend,
  type JobDequeueOptions,
  type WorkerBackend,
  type WorkerInboxOptions,
  type WorkerRegistrationOptions,
} from '../types/backend.js';
import { type InferJobArgs, type Job, type JobArgs } from '../types/job.js';
import { type JobFactory, type QueueEventEmitter } from '../types/queue.js';
import { type Task, type TaskManager } from '../types/task.js';
import {
  WorkerState,
  type WorkerInstance,
  type WorkerCommandHandler,
  type WorkerConfig,
  type WorkerId,
  type WorkerOptions,
} from '../types/worker.js';
import { WorkerCommandManager } from './command-manager.js';
import { WorkerTerminationError } from './errors.js';
import { Executor } from './executor.js';
import { WorkerLoop } from './loop.js';

/**
 * Default worker class.
 */
export class DefaultWorker<BaseJob extends Job<JobArgs>> implements WorkerInstance<BaseJob> {
  public static readonly DEFAULT_CONFIG = Object.freeze(<Partial<WorkerConfig>>{
    maxCapacity: 1,
    spareCapacity: 0,
    spareMinPriority: 1,
    heartbeatInterval: 10 * 1000,
    inboxCheckInterval: 60 * 1000,
    dequeueTimeout: 5 * 1000,
  });

  /**
   * ID of the worker, if registered
   */
  private _id: number | undefined = undefined;

  private _state: WorkerState = WorkerState.Offline;

  /**
   * Worker configuration
   */
  private _config: WorkerConfig;

  /**
   * Additional metadata (stored in database)
   */
  private _metadata: Record<string, any>;

  /**
   * Additional attachments (only available at runtime)
   */
  private attachments: Record<string, any>;

  private workerLoop: WorkerLoop<BaseJob> | null = null;
  private abortController = new AbortController();

  private lastHeartbeatAt = 0;
  private lastInboxCheck = 0;
  private commandManager: WorkerCommandManager;

  private finishedJobCount = 0;

  constructor(
    private workerBackend: WorkerBackend,
    options: WorkerOptions,
    private taskManager: TaskManager<BaseJob>,
    private jobFactory: JobFactory<BaseJob>,
    private jobBackend: JobBackend,
    private notifier: QueueEventEmitter<BaseJob>,
  ) {
    this._config = { ...options };
    this._metadata = { ...options.metadata };
    this.attachments = options.attachments ?? {};
    this.commandManager = new WorkerCommandManager(this, options.commands ?? {});
  }

  get id(): WorkerId | undefined {
    return this._id;
  }

  protected get isRegistered(): boolean {
    return this._id !== undefined;
  }

  get config(): Readonly<WorkerConfig> {
    return this._config;
  }

  async setConfig(config: Partial<WorkerConfig>): Promise<void> {
    this._config = { ...this._config, ...config };
    await this.heartbeat(true);
  }

  async setMetadata(key: string, value: any): Promise<void> {
    if (value !== null) {
      this._metadata[key] = value;
    } else {
      delete this._metadata[key];
    }
    await this.heartbeat(true);
  }

  getMetadata<T = any>(key: string): T | undefined {
    return this._metadata[key];
  }

  setAttachment(key: string, value: any): void {
    this.attachments[key] = value;
  }

  getAttachment<T = any>(key: string): T {
    if (this.attachments[key] === undefined) {
      throw new Error(`Attachment ${key} not found`);
    }
    return this.attachments[key];
  }

  get needsInboxCheck(): boolean {
    return this.lastInboxCheck + this._config.inboxCheckInterval < Date.now();
  }

  get needsHeartbeat(): boolean {
    return this.lastHeartbeatAt + this._config.heartbeatInterval < Date.now();
  }

  get state(): WorkerState {
    if (this.workerLoop) {
      // Update `this._state`
      this._state = this.workerLoop.hasRunningJobs ? WorkerState.Busy : WorkerState.Idle;
    }
    return this._state;
  }

  get isRunning(): boolean {
    return !!this.workerLoop;
  }

  async start(): Promise<this> {
    if (!this.workerLoop) {
      this.abortController = new AbortController();
      const workerLoop = (this.workerLoop = new WorkerLoop<BaseJob>(this));
      workerLoop.on('finished', (finished) => (this.finishedJobCount += finished));

      await this.register();
      (async () => {
        try {
          this._state = WorkerState.Idle;
          await workerLoop.run();
        } catch (e) {
          console.log(e);
        } finally {
          this.workerLoop = null;
          this._state = WorkerState.Online;
          await this.unregister();
        }
      })();
    }
    return this;
  }

  async stop(): Promise<void> {
    if (this.workerLoop) {
      await this.workerLoop.stop();
    }
  }

  async getNextExecutor(wait = 0, options: Partial<JobDequeueOptions> = {}): Promise<Executor<BaseJob> | null> {
    if (this.id === undefined) return null;
    const _options = <JobDequeueOptions>{
      queueNames: this._config.queueNames,
      ...options,
    };
    const taskNames = this.taskManager.getTaskNames();
    const jobInfo = await this.workerBackend.assignNextJob<InferJobArgs<BaseJob>>(this.id, taskNames, wait, _options);
    if (jobInfo === null) return null;
    return new Executor<BaseJob>(jobInfo, this, this.jobBackend, this.jobFactory, this.notifier);
  }

  async terminate(reason?: string): Promise<void> {
    if (this.workerLoop) {
      this.abortController.abort(new WorkerTerminationError(reason));
      await this.workerLoop.stop();
    }
  }

  get abortSignal(): AbortSignal {
    return this.abortController.signal;
  }

  getTask(taskName: string): Task<Job<JobArgs>> {
    return this.taskManager.getTask(taskName);
  }

  async register(): Promise<this> {
    if (!this.isRegistered) {
      const options: WorkerRegistrationOptions = {
        config: this._config,
        state: WorkerState.Online,
        finishedJobCount: this.finishedJobCount,
        metadata: this._metadata,
      };
      this._id = await this.workerBackend.registerWorker(options);
      this._state = WorkerState.Online;
      this.lastHeartbeatAt = Date.now();
    } else {
      await this.heartbeat(true);
    }

    return this;
  }

  async heartbeat(force: boolean = false): Promise<this> {
    if ((force || this.needsHeartbeat) && this.isRegistered) {
      const options: WorkerRegistrationOptions = {
        config: this._config,
        state: this.state,
        finishedJobCount: this.finishedJobCount,
        metadata: this._metadata,
      };
      await this.workerBackend.updateWorker(this._id!, options);
      this.lastHeartbeatAt = Date.now();
    }
    return this;
  }

  async processInbox(force: boolean = false): Promise<this> {
    if ((force || this.needsInboxCheck || this.needsHeartbeat) && this.isRegistered) {
      const options: WorkerInboxOptions = {
        state: this.state,
        finishedJobCount: this.finishedJobCount,
      };
      const commands = await this.workerBackend.checkWorkerInbox(this._id!, options);
      this.lastInboxCheck = this.lastHeartbeatAt = Date.now();
      await this.commandManager.runCommands(commands);
    }
    return this;
  }

  async unregister(): Promise<this> {
    if (this._id !== undefined) {
      await this.workerBackend.unregisterWorker(this._id);
      this._state = WorkerState.Offline;
      this._id = undefined;
    }
    return this;
  }

  addCommand(name: string, fn: WorkerCommandHandler): void {
    this.commandManager.addHandler(name, fn);
  }
}
