import { Executor } from '../worker/executor.js';
import { type JobDequeueOptions } from './backend.js';
import { type Job, type JobArgs, type JobId } from './job.js';
import { type Task } from './task.js';

/**
 * Class with methods of a `Worker` required within the `Executor`.
 */
export interface RunningWorker<BaseJob extends Job<JobArgs>> {
  /**
   * Worker id.
   */
  get id(): WorkerId | undefined;

  getMetadata<T = any>(key: string): T | undefined;
  getAttachment<T = any>(key: string): T;

  /**
   * Provides an abort signal to indicate that the worker is supposed to terminate.
   */
  get abortSignal(): AbortSignal;

  /**
   * Returns the given task (throws if it doesn't exist).
   */
  getTask(taskName: string): Task<BaseJob>;

  /**
   * Update the worker's lastSeen date.
   */
  heartbeat(force?: boolean): Promise<this>;

  /**
   * Update the worker's number of finished jobs
   */
  tickOff(): Promise<this>;
}

/**
 * Class with all methods that are required to configure and spawn a `Worker`.
 */
export interface WorkerInstance<BaseJob extends Job<JobArgs>> extends RunningWorker<BaseJob> {
  get config(): Readonly<WorkerConfig>;
  setConfig(config: Partial<WorkerConfig>): Promise<void>;
  setMetadata(key: string, value: any): Promise<void>;
  setAttachment(key: string, value: any): void;
  get governor(): WorkerGovenor;

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
   * Terminate worker loop (like `stop`, but sends an `AbortSignal`).
   */
  terminate(reason?: string): Promise<void>;

  /**
   * Check if worker is currently running.
   */
  get isRunning(): boolean;

  /**
   * Wait a given amount of time in milliseconds for a job, dequeue job object and transition from `pending` to
   * `running` state for the given worker, or return `null` if queues were empty.
   */
  getNextExecutor(wait?: number, options?: Partial<JobDequeueOptions>): Promise<Executor<BaseJob> | null>;

  /**
   * Register this worker in the backend (if not yet registered, otherwise just update its data).
   */
  register(): Promise<this>;

  /**
   * Check the worker's inbox, process remote control commands (if any), and updates its lastSeen date.
   */
  processInbox(force?: boolean): Promise<this>;

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
export type WorkerCommandHandler = (worker: WorkerInstance<any>, arg: WorkerCommandArg) => Promise<void>;
export type WorkerCommandDescriptor = { command: string; arg: WorkerCommandArg };

export interface WorkerOptions extends WorkerConfig {
  metadata: Record<string, any>;
  attachments: Record<string, any>;
  commands: Record<string, WorkerCommandHandler>;
  governor: WorkerGovenor;
}

export type WorkerGovenor = (worker: WorkerInstance<any>, runningJobCount: number) => Promise<boolean>;

export interface WorkerConfig {
  /**
   * The queues this worker would pick up.
   */
  queueNames: string[];
  /**
   * Maximal number of worker slots
   */
  maxCapacity: number;
  /**
   * Worker can reserve this number of slots.
   */
  reservedCapacity: number;
  /**
   * Minimal priority for the reserved slots.
   */
  reservedMinPriority: number;
  /**
   * Interval at which the worker's lastSeen date is updated.
   */
  heartbeatInterval: number;
  /**
   * Interval at which the worker's command inbox is checked (and lastSeen date is updated).
   */
  inboxCheckInterval: number;
  /**
   * Number of ms the worker waits for a new job before checking other chores.
   */
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
  metadata?: Record<string, any>[];
}

export interface WorkerInfo {
  id: WorkerId;

  config: WorkerConfig;
  state: WorkerState;

  finishedJobCount: number;
  metadata: Record<string, any>;

  startedAt: Date;
  lastSeenAt?: Date;

  jobIds: JobId[];
}
