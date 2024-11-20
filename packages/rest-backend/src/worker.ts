import {
  WorkerState,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type JobDequeueOptions,
  type JobInfo,
  type QueueEventEmitter,
  type WorkerBackend,
  type WorkerCommandDescriptor,
  type WorkerConfig,
  type WorkerId,
  type WorkerInfo,
  type WorkerRegistrationOptions,
  type WorkerUpdateOptions,
} from '@queuebone/core';

/**
 * Default worker class.
 */
export class Worker<BaseJob extends Job<JobArgs>> {
  private _workerInfo: WorkerInfo;

  private lastHeartbeatAt = 0;
  private lastInboxCheck = 0;

  constructor(
    private workerBackend: WorkerBackend,
    options: WorkerRegistrationOptions,
    private notifier: QueueEventEmitter<BaseJob>,
  ) {
    this._workerInfo = {
      id: undefined!,
      config: options.config,
      state: WorkerState.Offline,
      host: '',
      pid: 0,
      finishedJobCount: 0,
      metadata: options.metadata,
      startedAt: new Date(),
      jobIds: [],
    };
  }

  get workerInfo(): WorkerInfo {
    return this._workerInfo;
  }

  get id(): WorkerId | undefined {
    return this._workerInfo.id;
  }

  protected get isRegistered(): boolean {
    return this._workerInfo.id !== undefined;
  }

  get config(): Readonly<WorkerConfig> {
    return this._workerInfo.config;
  }

  get needsInboxCheck(): boolean {
    return this.lastInboxCheck + this.config.inboxCheckInterval < Date.now();
  }

  get needsHeartbeat(): boolean {
    return this.lastHeartbeatAt + this.config.heartbeatInterval < Date.now();
  }

  get state(): WorkerState {
    return this._workerInfo.state;
  }

  async getNextJob(taskNames: string[], minPriority: number): Promise<JobInfo<InferJobArgs<BaseJob>> | null> {
    if (this.id === undefined) return null;
    const _options = <JobDequeueOptions>{
      queueNames: this.config.queueNames,
      minPriority,
    };
    const { dequeueTimeout } = this.config;
    const jobInfo = await this.workerBackend.assignNextJob<InferJobArgs<BaseJob>>(
      this.id,
      taskNames,
      dequeueTimeout,
      _options,
    );

    if (jobInfo !== null) {
      if (this.notifier.listenerCount('job_started') > 0) {
        const event = {
          jobInfo,
        };
        this.notifier.emit('job_started', event);
      }
    }

    return jobInfo;
  }

  async register(): Promise<this> {
    if (!this.isRegistered) {
      const options: WorkerRegistrationOptions = {
        config: this.config,
        metadata: this._workerInfo.metadata,
      };
      this._workerInfo = await this.workerBackend.registerWorker(options);
      this.notifier.emit('worker_registered', { workerInfo: this.workerInfo });
      this.lastHeartbeatAt = Date.now();
    } else {
      await this.heartbeat(true);
    }
    return this;
  }

  async update(state: WorkerState): Promise<WorkerInfo> {
    this._workerInfo.state = state;
    const options: WorkerUpdateOptions = {
      config: this.config,
      state: this.state,
      finishedJobCount: this._workerInfo.finishedJobCount,
      metadata: this._workerInfo.metadata,
    };
    const workerInfo = await this.workerBackend.updateWorker(this.id!, options);
    if (workerInfo) {
      this._workerInfo.config = workerInfo.config;
      this._workerInfo.metadata = workerInfo.metadata;
    }
    this.lastHeartbeatAt = Date.now();
    return this._workerInfo;
  }

  async heartbeat(force: boolean = false): Promise<this> {
    if ((force || this.needsHeartbeat) && this.isRegistered) {
      const options: WorkerUpdateOptions = {
        config: this.config,
        state: this.state,
        finishedJobCount: this._workerInfo.finishedJobCount,
        metadata: this._workerInfo.metadata,
      };
      const workerInfo = await this.workerBackend.updateWorker(this.id!, options);
      if (workerInfo) {
        this._workerInfo.config = workerInfo.config;
        this._workerInfo.metadata = workerInfo.metadata;
      }
      this.lastHeartbeatAt = Date.now();
    }
    return this;
  }

  async getInbox(state: WorkerState): Promise<WorkerCommandDescriptor[]> {
    this._workerInfo.state = state;
    const options: WorkerUpdateOptions = {
      state: state,
      finishedJobCount: this._workerInfo.finishedJobCount,
    };
    return await this.workerBackend.checkWorkerInbox(this.id!, options);
  }

  async unregister(): Promise<this> {
    if (this.isRegistered) {
      await this.workerBackend.unregisterWorker(this.id!);
      this._workerInfo.state = WorkerState.Offline;
      this.notifier.emit('worker_unregistered', { workerId: this.id! });
      this._workerInfo.id = undefined!;
    }
    return this;
  }
}
