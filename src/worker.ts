import { type WorkerBackend, type WorkerInboxOptions, type WorkerRegistrationOptions } from './types/backend.js';
import { type QueueReader } from './types/queue.js';
import {
  type Worker,
  type WorkerCommandHandler,
  type WorkerConfig,
  type WorkerId,
  type WorkerInfo,
  WorkerState,
} from './types/worker.js';
import { WorkerCommandManager } from './worker/command-manager.js';
import { WorkerTerminationError } from './worker/errors.js';
import { WorkerLoop } from './worker/loop.js';

/**
 * Default worker class.
 */
export class DefaultWorker implements Worker {
  public static readonly FOREGROUND_QUEUE = '_foreground_queue';

  public static readonly DEFAULT_CONFIG = Object.freeze(<Partial<WorkerConfig>>{
    concurrency: 1,
    prefetchJobs: 0,
    prefetchMinPriority: 1,
    heartbeatInterval: 10 * 1000,
    inboxCheckInterval: 60 * 1000,
    dequeueTimeout: 5 * 1000,
  });

  /**
   * Additional metadata
   */
  private _metadata: Record<string, any> = {};

  private finishedJobCount = 0;

  private lastInboxCheck = 0;
  private lastHeartbeatAt = 0;

  private _state: WorkerState = WorkerState.Offline;
  private workerLoop: WorkerLoop | null = null;
  protected abortController = new AbortController();

  protected commandManager: WorkerCommandManager;

  private _id: number | undefined = undefined;

  constructor(
    protected queueReader: QueueReader,
    protected backend: WorkerBackend,
    protected _config: WorkerConfig,
    metadata: Record<string, any>,
    defaultCommands: Record<string, WorkerCommandHandler>,
  ) {
    this._metadata = { ...metadata };
    this.commandManager = new WorkerCommandManager(this, defaultCommands);
  }

  get id(): WorkerId | undefined {
    return this._id;
  }

  protected get isRegistered(): boolean {
    return this._id !== undefined;
  }

  async getInfo(): Promise<WorkerInfo | undefined> {
    if (this._id === undefined) return undefined;
    return await this.backend.getWorkerInfo(this._id);
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
      this.workerLoop = new WorkerLoop(this, this.queueReader);
      this.workerLoop.on('finished', (finished) => (this.finishedJobCount += finished));

      await this.register();
      (async () => {
        try {
          this._state = WorkerState.Idle;
          await this.workerLoop!.run();
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

  async terminate(reason?: string): Promise<void> {
    if (this.workerLoop) {
      this.abortController.abort(new WorkerTerminationError(reason));
      await this.workerLoop.stop();
    }
  }

  get abortSignal(): AbortSignal {
    return this.abortController.signal;
  }

  async register(): Promise<this> {
    if (!this.isRegistered) {
      const options: WorkerRegistrationOptions = {
        config: this._config,
        state: WorkerState.Online,
        finishedJobCount: this.finishedJobCount,
        metadata: this._metadata,
      };
      this._id = await this.backend.registerWorker(options);
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
      await this.backend.updateWorker(this._id!, options);
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
      const commands = await this.backend.checkWorkerInbox(this._id!, options);
      this.lastInboxCheck = this.lastHeartbeatAt = Date.now();
      await this.commandManager.runCommands(commands);
    }
    return this;
  }

  async unregister(): Promise<this> {
    if (this._id !== undefined) {
      await this.backend.unregisterWorker(this._id);
      this._state = WorkerState.Offline;
      this._id = undefined;
    }
    return this;
  }

  addCommand(name: string, fn: WorkerCommandHandler): void {
    this.commandManager.addHandler(name, fn);
  }
}
