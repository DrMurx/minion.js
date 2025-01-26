import {
  DefaultWorker,
  JobState,
  WorkerState,
  type ExecutorBackend,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDequeueOptions,
  type JobHandleBackend,
  type JobId,
  type QueueEventEmitter,
  type WorkerBackend,
  type WorkerCommandDescriptor,
  type WorkerConfig,
  type WorkerId,
  type WorkerInfo,
  type WorkerRegistrationOptions,
  type WorkerUpdateOptions,
} from '@queuebone/core';
import { hostname } from 'os';
import { ExecutorProxy } from './executor-proxy.js';
import { type WorkerProfile } from './profile.js';
import { type RemoteWorkerMetadata } from './types.js';

/**
 * The server side representation of a REST worker.
 */
export class WorkerProxy<BaseJob extends Job<JobArgs>> {
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

  private _startedAt = new Date();

  protected lastHeartbeatAt = 0;
  protected lastInboxCheck = 0;

  protected jobExecutors: Map<JobId, ExecutorProxy<BaseJob>> = new Map();
  protected deltaFinishedJobs = 0;

  constructor(
    protected profile: WorkerProfile<BaseJob>,
    protected queueNames: string[],
    protected workerBackend: WorkerBackend & ExecutorBackend & JobHandleBackend,
    protected notifier: QueueEventEmitter<BaseJob>,
  ) {
    this._config = { ...DefaultWorker.DEFAULT_CONFIG };

    this._metadata = {
      ':hostname': hostname(),
      ':pid': process.pid,
      ':profile': profile.name,
      ':profile_id': profile.id,
    };
  }

  isExpired(expireAfter: number) {
    return this.lastHeartbeatAt < expireAfter;
  }

  /**
   * Returns a version of `workerInfo` for the client. It has all internal information stripped.
   */
  get clientWorkerInfo(): WorkerInfo {
    // Filter all "internal" metadata values
    const metadata = Object.fromEntries(Object.entries(this._metadata).filter(([key]) => !key.startsWith(':')));

    const workerInfo: WorkerInfo = {
      id: this._id ?? 0,
      config: {
        queueNames: [],
        maxCapacity: this._config.maxCapacity,
        reservedCapacity: this._config.reservedCapacity,
        reservedMinPriority: this._config.reservedMinPriority,
        heartbeatInterval: 0,
        inboxCheckInterval: this._config.inboxCheckInterval,
        dequeueTimeout: 0,
      },
      state: this._state,
      finishedJobCount: 0,
      metadata,
      startedAt: this._startedAt,
      jobIds: [],
    };
    return workerInfo;
  }

  get id(): number | undefined {
    return this._id;
  }

  get state(): WorkerState {
    return this._state;
  }

  protected get isRegistered(): boolean {
    return this._id !== undefined;
  }

  get needsInboxCheck(): boolean {
    if (this._config.inboxCheckInterval === 0) return false;
    return this.lastInboxCheck + this._config.inboxCheckInterval < Date.now();
  }

  get needsHeartbeat(): boolean {
    if (this._config.heartbeatInterval === 0) return false;
    return this.lastHeartbeatAt + this._config.heartbeatInterval < Date.now();
  }

  async getNextExecutor(
    taskNames: string[],
    minPriority: number,
    serial: number,
  ): Promise<ExecutorProxy<BaseJob> | null> {
    if (this._id === undefined) return null;

    // Check whether we already have a executor stored with the given serial and return that instead.
    for (const [, executor] of this.jobExecutors) {
      if (serial === executor.serial) {
        return executor;
      }
    }

    const { queueNames, dequeueTimeout } = this._config;
    const _options = <JobDequeueOptions>{
      queueNames,
      minPriority,
    };
    const jobRecord = await this.workerBackend.assignNextJob<InferJobArgs<BaseJob>>(
      this._id,
      taskNames,
      dequeueTimeout,
      _options,
    );

    try {
      await this.heartbeat();
    } catch (_) {
      // do nothing
    }

    if (jobRecord === null) return null;

    const executor = new ExecutorProxy(jobRecord, this, serial, this.workerBackend, this.notifier);
    await executor.start();
    this.jobExecutors.set(jobRecord.id, executor);

    return executor;
  }

  async getRunningExecutor(jobId: JobId, attempt: number): Promise<ExecutorProxy<BaseJob> | undefined> {
    if (this._id === undefined) return undefined;

    const executor = this.jobExecutors.get(jobId) ?? (await this.recoverCurrentJob(jobId));
    if (executor === undefined || executor.attempt !== attempt) {
      return undefined;
    }
    return executor;
  }

  finishExecutor(executor: ExecutorProxy<BaseJob>): void {
    this.deltaFinishedJobs++;
    this.jobExecutors.delete(executor.jobRecord.id);
  }

  pruneExecutorProxies(expireAfter: number): void {
    for (const [jobId, jobExecutor] of this.jobExecutors) {
      if (jobExecutor.isExpired(expireAfter)) {
        this.jobExecutors.delete(jobId);
      }
    }
  }

  protected async recoverCurrentJob(jobId: JobId): Promise<ExecutorProxy<BaseJob> | undefined> {
    // TODO: This might be overly expensive because getJobInfo returns JobInfo but assignNextJob only JobRecord
    const jobInfo = await this.workerBackend.getJobInfo<InferJobArgs<BaseJob>>(jobId);
    if (jobInfo === undefined) return undefined;

    // Doesn't belong to this worker?
    if (jobInfo.workerId === undefined || jobInfo.workerId !== this._id) return undefined;

    // No need to recover if the job has already finished
    if ([JobState.Succeeded, JobState.Failed].includes(jobInfo.state)) return undefined;

    const executor = new ExecutorProxy(jobInfo, this, -1, this.workerBackend, this.notifier);
    this.jobExecutors.set(jobInfo.id, executor);

    return executor;
  }

  async register(meta: RemoteWorkerMetadata): Promise<this> {
    if (!this.isRegistered) {
      this._metadata[':remote_ip'] = meta.ip;
      this._metadata[':remote_hostname'] = meta.hostname;
      this._metadata[':remote_pid'] = meta.pid;
      const options: WorkerRegistrationOptions = {
        config: {
          ...DefaultWorker.DEFAULT_CONFIG,
          queueNames: this.queueNames,
          ...this.profile.config,
        },
        metadata: this._metadata,
      };

      // Check capacity before registering worker.
      // Note that this isn't an atomic operation, so there is a chance that another worker registers
      // at the same time.
      if (await this.profile.atMaxCapacity(this.workerBackend)) return this;

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

  async recover(id: WorkerId, meta: RemoteWorkerMetadata): Promise<this> {
    if (!this.isRegistered) {
      const options = {
        state: WorkerState.Online,
        metadata: {
          ':remote_ip': meta.ip,
          ':remote_hostname': meta.hostname,
          ':remote_pid': meta.pid,
        },
      };

      const workerInfo = await this.workerBackend.updateWorker(id, options);
      if (workerInfo === undefined) throw new Error(`Can't retrieve worker ${id}`);

      this._id = workerInfo.id;
      this._config = workerInfo.config;
      this._state = workerInfo.state;
      this._metadata = workerInfo.metadata;
      this.notifier.emit('worker_registered', { workerInfo });

      this.deltaFinishedJobs = 0;
      this.lastHeartbeatAt = Date.now();
    }
    return this;
  }

  async setState(state?: WorkerState): Promise<void> {
    if (state !== undefined) this._state = state;
    return this.heartbeat(true);
  }

  async heartbeat(force: boolean = false): Promise<void> {
    if ((force || this.needsHeartbeat) && this.isRegistered) {
      const options: WorkerUpdateOptions = {
        config: this._config,
        state: this._state,
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
  }

  async getInbox(updateState?: WorkerState): Promise<WorkerCommandDescriptor[]> {
    if (updateState !== undefined) this._state = updateState;
    const options: WorkerUpdateOptions = {
      state: this._state,
      deltaFinishedJobs: this.deltaFinishedJobs,
    };
    this.deltaFinishedJobs = 0;
    const commands = await this.workerBackend.checkWorkerInbox(this._id!, options);
    this.lastHeartbeatAt = Date.now();
    return commands;
  }

  async unregister(): Promise<this> {
    if (this._id !== undefined) {
      await this.workerBackend.unregisterWorker(this._id, this.deltaFinishedJobs);
      this.deltaFinishedJobs = 0;
      this._state = WorkerState.Offline;
      this.notifier.emit('worker_unregistered', { workerId: this._id });
      this._id = undefined!;
    }
    return this;
  }
}
