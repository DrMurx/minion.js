import { hostname } from 'os';
import { InvalidStateError } from '../errors.js';
import {
  type ExecutorBackend,
  type JobDequeueOptions,
  type WorkerBackend,
  type WorkerRegistrationOptions,
  type WorkerUpdateOptions,
} from '../types/backend.js';
import { type InferJobArgs, type Job, type JobArgs, type JobFactory } from '../types/job.js';
import { type QueueEventEmitter } from '../types/queue.js';
import { type Task, type TaskManager } from '../types/task.js';
import {
  WorkerState,
  type WorkerCommandHandler,
  type WorkerConfig,
  type WorkerGovenor,
  type WorkerId,
  type WorkerInstance,
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
  public static readonly DEFAULT_CONFIG = Object.freeze(<WorkerConfig>{
    queueNames: Object.freeze(['default']),
    maxCapacity: 1,
    reservedCapacity: 0,
    reservedMinPriority: 1,
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
  private _governor: WorkerGovenor;

  private deltaFinishedJobs = 0;

  constructor(
    private workerBackend: WorkerBackend,
    options: WorkerOptions,
    private taskManager: TaskManager<BaseJob>,
    private jobFactory: JobFactory<BaseJob>,
    private jobBackend: ExecutorBackend,
    private notifier: QueueEventEmitter<BaseJob>,
  ) {
    // Create a clean `WorkerConfig` object from the `WorkerOptions`
    const _options: Partial<WorkerOptions> = { ...options };
    delete _options.metadata;
    delete _options.attachments;
    delete _options.commands;
    delete _options.governor;
    this._config = _options as WorkerConfig;

    this._metadata = { ...options.metadata };
    this.attachments = options.attachments;
    this.commandManager = new WorkerCommandManager(this, options.commands);
    this._governor = options.governor;
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
      throw new InvalidStateError(`Attachment ${key} not found`);
    }
    return this.attachments[key];
  }

  get governor(): WorkerGovenor {
    return this._governor;
  }

  set governor(governor: WorkerGovenor) {
    this._governor = governor;
  }

  get needsInboxCheck(): boolean {
    if (this._config.inboxCheckInterval === 0) return false;
    return this.lastInboxCheck + this._config.inboxCheckInterval < Date.now();
  }

  get needsHeartbeat(): boolean {
    if (this._config.heartbeatInterval === 0) return false;
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
    if (this._id === undefined) return null;
    const _options = <JobDequeueOptions>{
      queueNames: this._config.queueNames,
      ...options,
    };
    const taskNames = this.taskManager.getTaskNames();
    const jobRecord = await this.workerBackend.assignNextJob<InferJobArgs<BaseJob>>(
      this._id,
      taskNames,
      wait,
      _options,
    );
    if (jobRecord === null) return null;
    return new Executor<BaseJob>(jobRecord, this, this.jobBackend, this.jobFactory, this.notifier);
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
        metadata: {
          ...this._metadata,
          ':hostname': hostname(),
          ':pid': process.pid,
        },
      };
      const workerInfo = await this.workerBackend.registerWorker(options);
      this._id = workerInfo.id;
      this._config = workerInfo.config;
      this._state = WorkerState.Online;
      this._metadata = workerInfo.metadata;
      this.notifier.emit('worker_registered', { workerInfo });
      this.deltaFinishedJobs = 0;
      this.lastHeartbeatAt = Date.now();
    } else {
      await this.heartbeat(true);
    }

    return this;
  }

  async heartbeat(force: boolean = false): Promise<this> {
    if ((force || this.needsHeartbeat) && this.isRegistered) {
      const options: WorkerUpdateOptions = {
        config: this._config,
        state: this.state,
        deltaFinishedJobs: this.deltaFinishedJobs,
        metadata: this._metadata,
      };
      this.deltaFinishedJobs = 0;
      const workerInfo = await this.workerBackend.updateWorker(this._id!, options);
      if (workerInfo) {
        this._config = workerInfo.config;
        this._metadata = workerInfo.metadata;
      }
      this.lastHeartbeatAt = Date.now();
    }
    return this;
  }

  async tickOff(): Promise<this> {
    this.deltaFinishedJobs++;
    return this;
  }

  async processInbox(force: boolean = false): Promise<this> {
    if ((force || this.needsInboxCheck || this.needsHeartbeat) && this.isRegistered) {
      const options: WorkerUpdateOptions = {
        state: this.state,
        deltaFinishedJobs: this.deltaFinishedJobs,
      };
      this.deltaFinishedJobs = 0;
      const commands = await this.workerBackend.checkWorkerInbox(this._id!, options);
      this.lastInboxCheck = this.lastHeartbeatAt = Date.now();
      await this.commandManager.runCommands(commands);
    }
    return this;
  }

  async unregister(): Promise<this> {
    if (this._id !== undefined) {
      await this.workerBackend.unregisterWorker(this._id, this.deltaFinishedJobs);
      this._state = WorkerState.Offline;
      this.notifier.emit('worker_unregistered', { workerId: this._id });
      this._id = undefined;
    }
    return this;
  }

  addCommand(name: string, fn: WorkerCommandHandler): void {
    this.commandManager.addHandler(name, fn);
  }
}
