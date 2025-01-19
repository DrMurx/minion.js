import {
  DefaultWorker,
  WorkerState,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDequeueOptions,
  type JobId,
  type JobRecord,
  type ListWorkersOptions,
  type QueueEventEmitter,
  type WorkerBackend,
  type WorkerCommandDescriptor,
  type WorkerConfig,
  type WorkerInfo,
  type WorkerRegistrationOptions,
  type WorkerUpdateOptions,
} from '@queuebone/core';
import { hostname } from 'os';
import { type ExecutorProxy } from './executor-proxy.js';
import { type WorkerProfileHolder } from './types.js';

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

  public jobExecutors: Map<JobId, ExecutorProxy<BaseJob>> = new Map();
  public finishedJobCount = 0;
  public lastSeenAt = Date.now();

  constructor(
    protected profile: WorkerProfileHolder<BaseJob>,
    protected queueNames: string[],
    protected workerBackend: WorkerBackend,
    protected notifier: QueueEventEmitter<BaseJob>,
  ) {
    this._config = { ...DefaultWorker.DEFAULT_CONFIG };

    this._metadata = {
      ':hostname': hostname(),
      ':pid': process.pid,
      ':profile': profile.name,
      ':profileId': profile.id,
    };
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

  async getNextJob(taskNames: string[], minPriority: number): Promise<JobRecord<InferJobArgs<BaseJob>> | null> {
    if (this._id === undefined) return null;
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

    await this.heartbeat();

    return jobRecord;
  }

  async register(ip: string): Promise<this> {
    if (!this.isRegistered) {
      this._metadata[':remote'] = ip;
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
      if (await this.profileCapacityExhausted()) return this;
      const workerInfo = await this.workerBackend.registerWorker(options);

      this._id = workerInfo.id;
      this._config = workerInfo.config;
      this._state = WorkerState.Online;
      this._metadata = workerInfo.metadata;
      this.notifier.emit('worker_registered', { workerInfo });

      this.finishedJobCount = 0;
      this.lastHeartbeatAt = Date.now();
    } else {
      await this.heartbeat(true);
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
        finishedJobCount: this.finishedJobCount,
        metadata: this._metadata,
      };
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
      finishedJobCount: this.finishedJobCount,
    };
    const commands = await this.workerBackend.checkWorkerInbox(this._id!, options);
    this.lastHeartbeatAt = Date.now();
    return commands;
  }

  async unregister(): Promise<this> {
    if (this._id !== undefined) {
      await this.workerBackend.unregisterWorker(this._id);
      this._state = WorkerState.Offline;
      this.notifier.emit('worker_unregistered', { workerId: this._id });
      this._id = undefined!;
    }
    return this;
  }

  protected async profileCapacityExhausted(): Promise<boolean> {
    const options: ListWorkersOptions = {
      state: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
      metadata: [
        {
          ':profileId': this.profile.id,
        },
      ],
    };
    const infos = await this.workerBackend.getWorkerInfos(0, 0, options);
    const activeWorkers = infos.total;
    return activeWorkers >= this.profile.maxWorkers;
  }
}
