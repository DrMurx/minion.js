import {
  JobPruneResult,
  type Backend,
  type JobDequeueOptions,
  type JobEnqueueOptions,
  type JobInfoList,
  type JobOptions,
  type WorkerInfoList,
  type WorkerPruneResult,
  type WorkerRegistrationOptions,
  type WorkerUpdateOptions,
} from '../types/backend.js';
import {
  JobState,
  type JobArgs,
  type JobId,
  type JobInfo,
  type JobRecord,
  type JobResult,
  type ListJobsOptions,
} from '../types/job.js';
import { DailyJobHistory, type QueueJobStatistics, type QueueStats } from '../types/queue-stats.js';
import {
  WorkerConfig,
  WorkerState,
  type ListWorkersOptions,
  type WorkerCommandArg,
  type WorkerCommandDescriptor,
  type WorkerId,
  type WorkerInfo,
} from '../types/worker.js';

const startTime = Date.now();

export class MemoryBackend implements Backend {
  public readonly name = 'Memory';

  protected nextJobId = 1;
  protected jobs: Set<JobRow> = new Set();
  protected jobInsertEvent?: () => void;
  protected nextWorkerId = 1;
  protected workers = new Set<WorkerRow>();

  private requeueHandler: (jobRecord: JobRecord<any>) => Promise<void> = async () => {};

  constructor() {}

  setRequeueHandler<Args extends JobArgs>(handler: (jobRecord: JobRecord<Args>) => Promise<void>): void {
    this.requeueHandler = handler;
  }

  async addJob<Args extends JobArgs>(
    taskName: string,
    args: Args,
    options: JobEnqueueOptions,
  ): Promise<JobRecord<Args>> {
    const job: JobRow = {
      ...options,
      id: this.nextJobId++,
      taskName,
      args,
      state: JobState.Pending,
      progress: 0,
      attempt: 1,
      delayUntil: new Date(Date.now() + options.delayFor),
      expiresAt: options.expireIn === undefined ? undefined : new Date(Date.now() + options.expireIn),
      createdAt: new Date(),
    };
    this.jobs.add(job);

    if (this.jobInsertEvent) this.jobInsertEvent();
    return { ...(job as JobRecord<Args>) };
  }

  async retryJob<Args extends JobArgs>(
    id: JobId,
    attempt: number,
    options: JobOptions,
  ): Promise<JobRecord<Args> | undefined> {
    const job = this.selectJob((j) => j.id === id && j.attempt === attempt);
    if (job === undefined) return undefined;
    const delayFor = options.delayFor ?? 0;
    job.queueName = options.queueName ?? job.queueName;
    job.state = JobState.Pending;
    job.priority = options.priority ?? job.priority;
    job.progress = 0;
    job.maxAttempts = options.maxAttempts ?? job.maxAttempts + 1;
    job.attempt++;
    job.parentJobIds = options.parentJobIds ?? job.parentJobIds;
    job.laxDependency = options.laxDependency ?? job.laxDependency;
    job.metadata = filterNull({ ...job.metadata, ...(options.metadata ?? {}) });
    job.delayUntil = new Date(Date.now() + delayFor);
    job.retriedAt = new Date();
    if (options.expireIn !== undefined) job.expiresAt = new Date(Date.now() + options.expireIn);

    if (this.jobInsertEvent) this.jobInsertEvent();
    return { ...(job as JobRecord<Args>) };
  }

  async cancelJob(id: JobId): Promise<boolean> {
    const job = this.selectJob((j) => j.id === id && j.state === JobState.Pending);
    if (job === undefined) return false;
    job.state = JobState.Canceled;
    return true;
  }

  async amendJobMetadata(
    id: JobId,
    attempt: number,
    records: Record<string, any>,
  ): Promise<Record<string, any> | undefined> {
    const job = this.selectJob((j) => j.id === id && j.attempt === attempt);
    if (job === undefined) return undefined;
    job.metadata = filterNull({ ...job.metadata, ...records });
    return { ...job.metadata };
  }

  async updateJobProgress(id: JobId, attempt: number, progress: number): Promise<boolean> {
    const job = this.selectJob((j) => j.id === id && j.attempt === attempt);
    if (job === undefined) return false;
    job.progress = progress;
    return true;
  }

  async markJobFinished(
    jobId: JobId,
    attempt: number,
    state: JobState.Succeeded | JobState.Failed | JobState.Aborted,
    result: JobResult,
  ): Promise<boolean> {
    const job = this.selectJob((j) => j.id === jobId && j.state === JobState.Running && j.attempt === attempt);
    if (job === undefined) return false;
    job.result = result;
    job.state = state;
    job.progress = state === JobState.Succeeded ? 1 : job.progress;
    job.finishedAt = new Date();
    if (state !== JobState.Succeeded) {
      await this.requeueHandler({ ...job });
    }
    return true;
  }

  async assignNextJob<Args extends JobArgs>(
    workerId: WorkerId,
    taskNames: string[],
    timeout: number,
    options: JobDequeueOptions,
  ): Promise<JobRecord<Args> | null> {
    for (let repeat = 1; ; repeat--) {
      const dequeueJobInfo = await this.tryAssignNextJob<Args>(workerId, taskNames, options);
      if (dequeueJobInfo !== null) return { ...dequeueJobInfo };
      if (timeout === 0 || repeat <= 0) return null;
      await this.waitForNewJobs(timeout);
    }
  }

  protected async tryAssignNextJob<Args extends JobArgs>(
    workerId: WorkerId,
    taskNames: string[],
    options: JobDequeueOptions,
  ): Promise<JobRecord<Args> | null> {
    const jobId = options.id;
    const minPriority = options.minPriority;
    const queueNames = Array.isArray(options.queueNames) ? options.queueNames : [options.queueNames];
    const now = new Date();

    const possibleJobs = this.selectJobs(
      (j) =>
        j.id === (jobId ?? j.id) &&
        queueNames.includes(j.queueName) &&
        taskNames.includes(j.taskName) &&
        j.state === JobState.Pending &&
        j.priority >= (minPriority ?? j.priority) &&
        (j.parentJobIds.length === 0 ||
          this.selectJob(
            (pj) =>
              j.parentJobIds.includes(pj.id) &&
              ((pj.state === JobState.Pending && (pj.expiresAt === undefined || pj.expiresAt > now)) ||
                pj.state === JobState.Running ||
                ([
                  JobState.Failed,
                  JobState.Aborted,
                  JobState.Abandoned,
                  JobState.Unattended,
                  JobState.Canceled,
                ].includes(pj.state) &&
                  !j.laxDependency)),
          ) === undefined) &&
        j.delayUntil <= now &&
        (j.expiresAt === undefined || j.expiresAt > now),
    );
    if (possibleJobs.size === 0) return null;
    const jobs = [...possibleJobs].sort((a, b) => {
      const priority = b.priority - a.priority;
      return priority !== 0 ? priority : a.id - b.id;
    });
    const job = jobs[0];
    job.state = JobState.Running;
    job.progress = 0;
    job.workerId = workerId;
    job.startedAt = now;
    return job as JobRecord<Args>;
  }

  /**
   * Wait a given amount of time for a new job to become available.
   */
  protected async waitForNewJobs(timeout: number): Promise<boolean> {
    try {
      let timer;
      const timeoutPromise = new Promise((_, rej) => (timer = setTimeout(rej, timeout)));

      const notifyPromise = new Promise<void>((res) => {
        this.jobInsertEvent = res;
      });

      await Promise.race([notifyPromise, timeoutPromise]);
      clearTimeout(timer);
      return true;
    } catch (_) {
      return false;
    } finally {
      this.jobInsertEvent = undefined;
    }
  }

  async removeJob(id: JobId): Promise<boolean> {
    const job = this.selectJob((j) => j.id === id);
    if (job === undefined) return false;
    this.jobs.delete(job);
    return true;
  }

  async pruneJobs<Args extends JobArgs>(
    unattendedPeriod: number,
    expungePeriod: number,
    ignoreQueues: string[],
  ): Promise<JobPruneResult<Args>> {
    const now = new Date();

    const expiredJobs = this.selectJobs((j) => j.state === JobState.Pending && j.expiresAt! <= now);
    for (const job of expiredJobs) {
      this.jobs.delete(job);
    }

    const expungedJobs = this.selectJobs(
      (j) => j.state === JobState.Succeeded && now.getTime() - j.finishedAt!.getTime() >= expungePeriod,
    );
    for (const job of expungedJobs) {
      this.jobs.delete(job);
    }

    const unattendedJobs = this.selectJobs(
      (j) => j.state === JobState.Pending && now.getTime() - j.delayUntil.getTime() > unattendedPeriod,
    );
    for (const job of unattendedJobs) {
      job.state = JobState.Unattended;
    }

    const abandonedJobs = this.selectJobs(
      (j) =>
        j.state === JobState.Running &&
        !ignoreQueues.includes(j.queueName) &&
        this.selectWorker(
          (w) => w.id === j.workerId && [WorkerState.Online, WorkerState.Busy, WorkerState.Idle].includes(w.state),
        ) === undefined,
    );
    for (const job of abandonedJobs) {
      job.result = { name: 'WorkerGoneError', message: 'Worker went away' };
      job.state = JobState.Abandoned;
      job.finishedAt = now;
    }

    await Promise.allSettled([...abandonedJobs].map((jobRecord) => this.requeueHandler(jobRecord)));

    return {
      expiredJobs: [...expiredJobs] as JobRecord<Args>[],
      expungedJobs: [...expungedJobs] as JobRecord<Args>[],
      abandonedJobs: [...abandonedJobs] as JobRecord<Args>[],
      unattendedJobs: [...unattendedJobs] as JobRecord<Args>[],
    };
  }

  async getJobInfo<Args extends JobArgs>(jobId: JobId): Promise<JobInfo<Args> | undefined> {
    const job = this.selectJob((j) => j.id === jobId);
    if (job === undefined) return undefined;
    const childJobIds: JobId[] = [...this.selectJobs((j) => j.parentJobIds.includes(jobId))].map((j) => j.id);
    const jobInfo: JobInfo<Args> = {
      ...job,
      args: job.args as Args,
      childJobIds,
      time: new Date(),
    };
    return jobInfo;
  }

  /**
   * Returns the information about jobs in batches.
   */
  async getJobInfos<Args extends JobArgs>(
    offset: number,
    limit: number,
    options: ListJobsOptions,
  ): Promise<JobInfoList<Args>> {
    const now = new Date();
    const possibleJobs = this.selectJobs(
      (j) =>
        (options.afterId === undefined || j.id > options.afterId) &&
        (options.ids === undefined || options.ids.includes(j.id)) &&
        (options.queueNames === undefined || options.queueNames.includes(j.queueName)) &&
        (options.taskNames === undefined || options.taskNames.includes(j.taskName)) &&
        (options.states === undefined || options.states.includes(j.state)) &&
        (options.metadata === undefined ||
          Object.entries(options.metadata).every(([key, value]) => j.metadata[key] === value)) &&
        (j.state === JobState.Pending || j.expiresAt === undefined || j.expiresAt > now),
    );
    const jobs = [...possibleJobs].sort((a, b) => a.id - b.id).slice(offset, offset + limit);
    return {
      jobs: jobs.map((job) => {
        // TODO: Child records
        const childJobIds: JobId[] = [];
        return {
          ...job,
          args: job.args as Args,
          childJobIds,
          time: now,
        };
      }),
      total: possibleJobs.size,
    };
  }

  async registerWorker(options: WorkerRegistrationOptions): Promise<WorkerInfo> {
    const worker: WorkerRow = {
      id: this.nextWorkerId++,
      config: options.config,
      state: WorkerState.Online,
      finishedJobCount: 0,
      metadata: options.metadata,
      inbox: [],
      startedAt: new Date(),
      lastSeenAt: new Date(),
    };
    this.workers.add(worker);
    return {
      ...worker,
      jobIds: [],
    };
  }

  async updateWorker(workerId: WorkerId, options: WorkerUpdateOptions): Promise<WorkerInfo | undefined> {
    const worker = this.selectWorker((w) => w.id === workerId);
    if (worker === undefined) return undefined;
    worker.config = options.config ?? worker.config;
    worker.state = options.state ?? worker.state;
    worker.finishedJobCount = options.finishedJobCount ?? worker.finishedJobCount;
    worker.metadata = filterNull({ ...worker.metadata, ...(options.metadata ?? {}) });
    worker.lastSeenAt = new Date();
    return {
      ...worker,
      jobIds: [],
    };
  }

  async checkWorkerInbox(workerId: WorkerId, options: WorkerUpdateOptions): Promise<WorkerCommandDescriptor[]> {
    const worker = this.selectWorker((w) => w.id === workerId);
    if (worker === undefined) return [];
    const inbox = worker.inbox;

    worker.config = options.config ?? worker.config;
    worker.state = options.state ?? worker.state;
    worker.finishedJobCount = options.finishedJobCount ?? worker.finishedJobCount;
    worker.metadata = filterNull({ ...worker.metadata, ...(options.metadata ?? {}) });
    worker.inbox = [];
    worker.lastSeenAt = new Date();

    return inbox;
  }

  async unregisterWorker(id: WorkerId): Promise<boolean> {
    const worker = this.selectWorker((w) => w.id === id);
    if (worker === undefined) return false;
    worker.state = WorkerState.Offline;
    return true;
  }

  async pruneWorkers(lostTimeout: number): Promise<WorkerPruneResult> {
    const workers = this.selectWorkers(
      (w) =>
        [WorkerState.Online, WorkerState.Idle, WorkerState.Busy].includes(w.state) &&
        Date.now() - w.lastSeenAt!.getTime() > lostTimeout,
    );
    const lostWorkers = [];
    for (const worker of workers) {
      worker.state = WorkerState.Lost;
      lostWorkers.push({
        ...worker,
        jobIds: [],
      });
    }
    return {
      lostWorkers,
    };
  }

  async getWorkerInfo(workerId: WorkerId): Promise<WorkerInfo | undefined> {
    const worker = this.selectWorker((w) => w.id === workerId);
    if (worker === undefined) return undefined;
    const jobs = this.selectJobs((j) => j.state === JobState.Running && j.workerId === workerId);
    const jobIds = [...jobs].map((j) => j.id);
    return {
      ...worker,
      jobIds,
    };
  }

  async getWorkerInfos(offset: number, limit: number, options: ListWorkersOptions): Promise<WorkerInfoList> {
    const possibleWorkers = this.selectWorkers(
      (w) =>
        (options.afterId === undefined || w.id > options.afterId) &&
        (options.ids === undefined || options.ids.includes(w.id)) &&
        (options.state === undefined || options.state.includes(w.state)) &&
        (options.metadata === undefined ||
          Object.entries(options.metadata).every(([key, value]) => w.metadata[key] === value)),
    );
    const workers = [...possibleWorkers].sort((a, b) => a.id - b.id).slice(offset, offset + limit);
    return {
      workers: workers.map((worker) => {
        const jobs = this.selectJobs((j) => j.state === JobState.Running && j.workerId === worker.id);
        const jobIds = [...jobs].map((j) => j.id);
        return {
          ...worker,
          jobIds,
        };
      }),
      total: possibleWorkers.size,
    };
  }

  async sendWorkerCommand(command: string, arg: WorkerCommandArg, options: ListWorkersOptions): Promise<boolean> {
    const workers = this.selectWorkers(
      (w) =>
        (options.afterId === undefined || w.id > options.afterId) &&
        (options.ids === undefined || options.ids.includes(w.id)) &&
        (options.state === undefined || options.state.includes(w.state)) &&
        (options.metadata === undefined ||
          Object.entries(options.metadata).every(([key, value]) => w.metadata[key] === value)),
    );
    if (workers.size === 0) return false;
    const descriptor: WorkerCommandDescriptor = { command, arg };
    for (const worker of workers) {
      worker.inbox = [...worker.inbox, descriptor];
    }
    return true;
  }

  async getJobHistory(): Promise<QueueJobStatistics> {
    const stateToFieldMap: Record<JobState, keyof DailyJobHistory | null> = {
      [JobState.Pending]: null,
      [JobState.Running]: null,
      [JobState.Succeeded]: 'succeededJobs',
      [JobState.Failed]: 'failedJobs',
      [JobState.Canceled]: null,
      [JobState.Aborted]: 'abortedJobs',
      [JobState.Abandoned]: 'abandonedJobs',
      [JobState.Unattended]: 'unattendedJobs',
    };

    const now = Date.now();

    // Prepare 24 history slots
    const history: DailyJobHistory[] = [];
    for (let i = 0; i <= 23; i++) {
      history.push({
        epoch: Math.round(now / 1000 - i * 86400),
        succeededJobs: 0,
        failedJobs: 0,
        abortedJobs: 0,
        abandonedJobs: 0,
        unattendedJobs: 0,
      });
    }

    const jobs = this.selectJobs((job) => job.finishedAt! >= new Date(now - 86400 * 1000));

    for (const job of jobs) {
      const slot = Math.round((now - job.finishedAt!.getTime()) / (24 * 1000));
      const field = stateToFieldMap[job.state];
      if (field === null || slot >= 24) continue;
      history[slot][field]++;
    }

    return { daily: history };
  }

  async getStats(): Promise<QueueStats> {
    const now = new Date();
    const runningJobs = this.selectJobs((j) => j.state === JobState.Running);
    const onlineWorkers = this.selectWorkers((w) =>
      [WorkerState.Online, WorkerState.Idle, WorkerState.Busy].includes(w.state),
    );
    const busyWorkers = new Set([...runningJobs].map((j) => j.workerId));

    return {
      enqueuedJobs: this.nextJobId - 1,
      pendingJobs: this.selectJobs((j) => j.state === JobState.Pending).size,
      scheduledJobs: this.selectJobs((j) => j.state === JobState.Pending && j.delayUntil > now).size,
      runningJobs: runningJobs.size,
      succeededJobs: this.selectJobs((j) => j.state === JobState.Succeeded).size,
      failedJobs: this.selectJobs((j) => j.state === JobState.Failed).size,
      abortedJobs: this.selectJobs((j) => j.state === JobState.Aborted).size,
      abandonedJobs: this.selectJobs((j) => j.state === JobState.Abandoned).size,
      unattendedJobs: this.selectJobs((j) => j.state === JobState.Unattended).size,
      canceledJobs: this.selectJobs((j) => j.state === JobState.Canceled).size,

      offlineWorkers: this.selectWorkers((w) => w.state === WorkerState.Offline).size,
      onlineWorkers: onlineWorkers.size,
      busyWorkers: busyWorkers.size,
      idleWorkers: onlineWorkers.size - busyWorkers.size,
      lostWorkers: this.selectWorkers((w) => w.state === WorkerState.Lost).size,

      queueboneVersion: '?',

      backendName: this.name,
      backendVersion: '1.0.0',
      backendUptime: Date.now() - startTime,
    };
  }

  async start(): Promise<void> {
    // do nothing
  }

  async end(): Promise<void> {
    // do nothing
  }

  async reset(): Promise<void> {
    this.jobs.clear();
    this.nextJobId = 1;
    this.workers.clear();
    this.nextWorkerId = 1;
    this.jobInsertEvent = undefined;
  }

  protected selectJob(predicate: (j: JobRow) => boolean): JobRow | undefined {
    for (const job of this.jobs) {
      const match = predicate(job);
      if (match) return job;
    }
    return undefined;
  }

  protected selectJobs(predicate: (j: JobRow) => boolean): Set<JobRow> {
    const result = new Set<JobRow>();
    for (const job of this.jobs) {
      const match = predicate(job);
      if (match) result.add(job);
    }
    return result;
  }

  protected selectWorker(predicate: (w: WorkerRow) => boolean): WorkerRow | undefined {
    for (const worker of this.workers) {
      if (predicate(worker)) return worker;
    }
    return undefined;
  }

  protected selectWorkers(predicate: (w: WorkerRow) => boolean): Set<WorkerRow> {
    const result = new Set<WorkerRow>();
    for (const worker of this.workers) {
      if (predicate(worker)) result.add(worker);
    }
    return result;
  }
}

function filterNull(object: Record<string, any>): Record<string, any> {
  const result: Record<string, any> = {};
  for (const key in object) {
    if (object[key] !== null) result[key] = object[key];
  }
  return result;
}

type JobRow = {
  id: JobId;

  queueName: string;
  taskName: string;
  args: JobArgs;
  result?: JobResult;

  state: JobState;
  priority: number;
  progress: number;
  maxAttempts: number;
  attempt: number;

  parentJobIds: JobId[];
  laxDependency: boolean;

  workerId?: WorkerId;
  metadata: Record<string, any>;

  delayUntil: Date;
  startedAt?: Date;
  retriedAt?: Date;
  finishedAt?: Date;

  createdAt: Date;
  expiresAt?: Date;
};

type WorkerRow = {
  id: WorkerId;

  config: WorkerConfig;
  state: WorkerState;

  finishedJobCount: number;
  metadata: Record<string, any>;
  inbox: WorkerCommandDescriptor[];

  startedAt: Date;
  lastSeenAt?: Date;
};
