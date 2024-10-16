import { type JobId } from './job.js';

export interface Worker {
  /**
   * Worker id.
   */
  get id(): WorkerId | undefined;

  /**
   * Get worker information.
   */
  getInfo(): Promise<WorkerInfo | undefined>;
  get config(): Readonly<WorkerConfig>;
  setConfig(config: Partial<WorkerConfig>): Promise<void>;
  setMetadata(key: string, value: any): Promise<void>;

  get state(): WorkerState;

  /**
   * Register and start worker loop.
   */
  start(): Promise<this>;

  /**
   * Stop worker loop.
   */
  stop(): Promise<void>;

  /**
   * Terminate worker loop (like `Worker.stop`, but sends an `AbortSignal`s).
   */
  terminate(reason?: string): Promise<void>;

  /**
   * Check if worker is currently running.
   */
  get isRunning(): boolean;

  /**
   * Provides an abort signal to indicate that the worker is supposed to terminate.
   */
  get abortSignal(): AbortSignal;

  /**
   * Register this worker in the backend (if not yet registered, otherwise just update its data).
   */
  register(): Promise<this>;

  /**
   * Check the worker's inbox, process remote control commands (if any), and updates its lastSeen date.
   */
  processInbox(force?: boolean): Promise<this>;

  /**
   * Update the worker's lastSeen date.
   */
  heartbeat(force?: boolean): Promise<this>;

  /**
   * Unregister worker.
   */
  unregister(): Promise<this>;

  /**
   * Register a worker remote control command.
   */
  addCommand(name: string, fn: WorkerCommandHandler): void;
}

export type WorkerId = number;

export type WorkerCommandArg = Record<string, any> & { [Symbol.iterator]?: never };
export type WorkerCommandHandler = (worker: Worker, arg: WorkerCommandArg) => Promise<void>;
export type WorkerCommandDescriptor = { command: string; arg: WorkerCommandArg };

export interface WorkerOptions {
  config?: Partial<WorkerConfig>;
  metadata?: Record<string, any>;
  commands?: Record<string, WorkerCommandHandler>;
}

export interface WorkerConfig {
  queueNames: string[];
  concurrency: number;
  prefetchJobs: number;
  prefetchMinPriority: number;
  heartbeatInterval: number;
  inboxCheckInterval: number;
  dequeueTimeout: number;
}

export enum WorkerState {
  Offline = 'offline',
  Online = 'online',
  Idle = 'idle',
  Busy = 'busy',
  Lost = 'lost',
}

export interface ListWorkersOptions {
  ids?: WorkerId[];
  afterId?: number;
  state?: WorkerState[];
  metadata?: string[];
}

export interface WorkerInfo {
  id: WorkerId;

  config: WorkerConfig;
  state: WorkerState;
  host: string;
  pid: number;

  finishedJobCount: number;
  metadata: Record<string, any>;

  startedAt: Date;
  lastSeenAt?: Date;

  jobs: JobId[];
}
