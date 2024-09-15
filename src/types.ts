import type {Job} from './job.js';
import type {Worker} from './worker.js';

export interface MinionOptions extends Partial<RepairOptions> {
}

export interface RepairOptions {
  /**
   * Amount of time in milliseconds after which workers without a heartbeat will be considered missing and removed from
   * the registry by `minion.repair()`, defaults to `1800000` (30 minutes).
   */
  missingAfter: number;

  /**
   * Amount of time in milliseconds after which jobs that have reached the state `finished` and have no unresolved
   * dependencies will be removed automatically by `minion.repair()`, defaults to `172800000` (2 days). It is not
   * recommended to set this value below 2 days.
   */
  removeAfter: number;

  /**
   * Amount of time in milliseconds after which jobs that have not been processed will be considered stuck by
   * `minion.repair()` and transition to the `failed` state, defaults to `172800000` (2 days).
   */
  stuckAfter: number;
}

export type MinionBackendConstructor = new (config: any, calcBackoff: MinionBackoffStrategy) => MinionBackend;
export type MinionBackoffStrategy = (retries: number) => number;

export interface MinionBackend {
  name: string;

  addJob: (task: string, args?: MinionArgs, options?: EnqueueOptions) => Promise<MinionJobId>;
  getNextJob: (id: MinionWorkerId, tasks: string[], wait: number, options: DequeueOptions) => Promise<DequeuedJob | null>;
  markJobFailed: (id: MinionJobId, retries: number, result?: any) => Promise<boolean>;
  markJobFinished: (id: MinionJobId, retries: number, result?: any) => Promise<boolean>;
  retryJob: (id: MinionJobId, retries: number, options: RetryOptions) => Promise<boolean>;
  removeJob: (id: MinionJobId) => Promise<boolean>;
  getJobInfos: (offset: number, limit: number, options?: ListJobsOptions) => Promise<JobList>;
  getJobHistory: () => Promise<any>;

  addNotes: (id: MinionJobId, notes: Record<string, any>) => Promise<boolean>;

  registerWorker: (id?: MinionWorkerId, options?: RegisterWorkerOptions) => Promise<number>;
  unregisterWorker: (id: MinionWorkerId) => Promise<void>;
  getWorkers: (offset: number, limit: number, options?: ListWorkersOptions) => Promise<WorkerList>;
  notifyWorkers: (command: string, args?: any[], ids?: MinionJobId[]) => Promise<boolean>;
  getWorkerNotifications: (id: MinionWorkerId) => Promise<Array<[string, ...any[]]>>;

  lock: (name: string, duration: number, options?: LockOptions) => Promise<boolean>;
  unlock: (name: string) => Promise<boolean>;
  getLocks: (offset: number, limit: number, options?: ListLocksOptions) => Promise<LockList>;

  stats: () => Promise<any>;
  repair: (options: RepairOptions) => Promise<void>;
  updateSchema: () => Promise<void>;
  reset: (options: ResetOptions) => Promise<void>;

  end: () => Promise<void>;
}

export type MinionArgs = any[];
export type MinionCommand = (worker: MinionWorker, ...args: any[]) => Promise<void>;
export type MinionStates = 'inactive' | 'active' | 'failed' | 'finished';
export type MinionJob = Job;
export type MinionJobId = number;
export type MinionWorkerId = number;
export type MinionTask = (job: MinionJob, ...args: MinionArgs) => Promise<void>;
export type MinionWorker = Worker;

export interface MinionHistory {
  daily: DailyHistory[];
}

export interface MinionStats {
  active_locks: number;
  active_jobs: number;
  active_workers: number;
  delayed_jobs: number;
  enqueued_jobs: number;
  failed_jobs: number;
  finished_jobs: number;
  inactive_jobs: number;
  inactive_workers: number;
  uptime: number;
  workers: number;
}

export interface MinionStatus {
  commandInterval: number;
  dequeueTimeout: number;
  heartbeatInterval: number;
  jobs: number;
  performed: number;
  queues: string[];
  repairInterval: number;
  spare: number;
  spareMinPriority: number;
  [key: string]: any;
}

export interface DequeueOptions {
  id?: MinionJobId;
  minPriority?: number;
  queues?: string[];
}

export interface EnqueueOptions {
  attempts?: number;
  delay?: number;
  expire?: number;
  lax?: boolean;
  notes?: Record<string, any>;
  parents?: MinionJobId[];
  priority?: number;
  queue?: string;
}

export interface ListJobsOptions {
  before?: number;
  ids?: MinionJobId[];
  notes?: string[];
  queues?: string[];
  states?: MinionStates[];
  tasks?: string[];
}

export interface ListLocksOptions {
  names?: string[];
}

export interface ListWorkersOptions {
  before?: number;
  ids?: MinionWorkerId[];
}

export interface LockOptions {
  limit?: number;
}

export interface RegisterWorkerOptions {
  status?: Record<string, any>;
}

export interface ResetOptions {
  all?: boolean;
  locks?: boolean;
}

export interface RetryOptions {
  attempts?: number;
  delay?: number;
  expire?: number;
  lax?: boolean;
  parents?: MinionJobId[];
  priority?: number;
  queue?: string;
}

export interface WorkerOptions {
  commands?: Record<string, MinionCommand>;
  status?: Record<string, any>;
}

export interface DailyHistory {
  epoch: number;
  failed_jobs: number;
  finished_jobs: number;
}

export interface DequeuedJob {
  id: MinionJobId;
  args: MinionArgs;
  retries: number;
  task: string;
}

export interface JobInfo {
  args: MinionArgs;
  attempts: number;
  children: MinionJobId[];
  created: Date;
  delayed: Date;
  expires: Date;
  finished: Date;
  id: MinionJobId;
  lax: boolean;
  notes: Record<string, any>;
  parents: MinionJobId[];
  priority: number;
  queue: string;
  result: any;
  retried: Date;
  retries: number;
  started: Date;
  state: MinionStates;
  task: string;
  time: Date;
  worker: MinionWorkerId;
}

export interface JobList {
  jobs: JobInfo[];
  total: number;
}

export interface LockInfo {
  expires: Date;
  name: string;
}

export interface LockList {
  locks: LockInfo[];
  total: number;
}

export interface ResultOptions {
  interval?: number;
  signal?: AbortSignal;
}

export interface WorkerInfo {
  id: MinionWorkerId;
  host: string;
  jobs: MinionJobId[];
  notified?: Date;
  pid: number;
  started: Date;
  status: Record<string, any>;
}

export interface WorkerList {
  workers: WorkerInfo[];
  total: number;
}
