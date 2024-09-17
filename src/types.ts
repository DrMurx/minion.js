import type { Job } from './job.js';
import type { Worker } from './worker.js';

export interface MinionOptions extends Partial<RepairOptions> {
}

export interface RepairOptions {
  /**
   * Amount of time in milliseconds after which workers without a heartbeat will be considered missing and removed from
   * the registry by `minion.repair()`, defaults to `1800000` (30 minutes).
   */
  missingAfter: number;

  /**
   * Amount of time in milliseconds after which jobs that have reached the state `succeeded` and have no unresolved
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

  addJob: (taskName: string, args?: MinionArgs, options?: EnqueueOptions) => Promise<MinionJobId>;
  getNextJob: (id: MinionWorkerId, taskNames: string[], wait: number, options: DequeueOptions) => Promise<DequeuedJob | null>;
  markJobFailed: (id: MinionJobId, retries: number, result?: any) => Promise<boolean>;
  markJobSucceeded: (id: MinionJobId, retries: number, result?: any) => Promise<boolean>;
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

  stats: () => Promise<any>;
  repair: (options: RepairOptions) => Promise<void>;
  updateSchema: () => Promise<void>;
  reset: (options: ResetOptions) => Promise<void>;

  end: () => Promise<void>;
}

export type MinionArgs = any[];
export type MinionCommand = (worker: MinionWorker, ...args: any[]) => Promise<void>;
export type MinionStates = 'pending' | 'running' | 'failed' | 'succeeded';
export type MinionJob = Job;
export type MinionJobId = number;
export type MinionWorkerId = number;
export type MinionTask = (job: MinionJob, ...args: MinionArgs) => Promise<void>;
export type MinionWorker = Worker;

export interface MinionHistory {
  daily: DailyHistory[];
}

export interface MinionStats {
  enqueuedJobs: number;
  pendingJobs: number;
  delayedJobs: number;
  runningJobs: number;
  succeededJobs: number;
  failedJobs: number;

  workers: number;
  busyWorkers: number;
  idleWorkers: number;

  uptime: number;
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
  queueNames?: string[];
}

export interface EnqueueOptions {
  attempts?: number;
  delayUntil?: number;
  expiresAt?: number;
  laxDependency?: boolean;
  notes?: Record<string, any>;
  parentJobIds?: MinionJobId[];
  priority?: number;
  queueName?: string;
}

export interface ListJobsOptions {
  beforeId?: number;
  ids?: MinionJobId[];
  notes?: string[];
  queueNames?: string[];
  states?: MinionStates[];
  taskNames?: string[];
}

export interface ListWorkersOptions {
  beforeId?: number;
  ids?: MinionWorkerId[];
}

export interface RegisterWorkerOptions {
  status?: Record<string, any>;
}

export interface ResetOptions {
  all?: boolean;
}

export interface RetryOptions {
  attempts?: number;
  delayUntil?: number;
  expireAt?: number;
  laxDependency?: boolean;
  parentJobIds?: MinionJobId[];
  priority?: number;
  queueName?: string;
}

export interface WorkerOptions {
  commands?: Record<string, MinionCommand>;
  status?: Record<string, any>;
}

export interface DailyHistory {
  epoch: number;
  failedJobs: number;
  succeededJobs: number;
}

export interface DequeuedJob {
  id: MinionJobId;
  args: MinionArgs;
  retries: number;
  taskName: string;
}

export interface JobInfo {
  id: MinionJobId;

  queueName: string;
  taskName: string;
  args: MinionArgs;
  result: any;

  state: MinionStates;
  priority: number;
  attempts: number;
  retries: number;

  parentJobIds: MinionJobId[];
  childJobIds: MinionJobId[];
  laxDependency: boolean;

  workerId: MinionWorkerId;
  notes: Record<string, any>;

  delayUntil: Date;
  startedAt: Date;
  retriedAt: Date;
  finishedAt: Date;

  createdAt: Date;
  expiresAt: Date;

  time: Date;
}

export interface JobList {
  jobs: JobInfo[];
  total: number;
}

export interface ResultOptions {
  interval?: number;
  signal?: AbortSignal;
}

export interface WorkerInfo {
  id: MinionWorkerId;

  host: string;
  pid: number;

  status: Record<string, any>;
  jobs: MinionJobId[];

  startedAt: Date;
  lastSeenAt?: Date;
}

export interface WorkerList {
  workers: WorkerInfo[];
  total: number;
}
