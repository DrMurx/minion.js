import {
  WorkerState,
  type Backend,
  type Job,
  type JobArgs,
  type ListWorkersOptions,
  type Queue,
  type WorkerBackend,
  type WorkerConfig,
  type WorkerId,
} from '@queuebone/core';
import { timingSafeCompare } from './compare.js';
import { type RestWorkerProfileConfig } from './config.js';
import { DefaultProfileManager } from './profile-manager.js';
import { type RemoteWorkerMetadata, type WorkerProfileId } from './types.js';
import { WorkerProxy } from './worker-proxy.js';

export class WorkerProfile<BaseJob extends Job<JobArgs>> {
  protected _id: WorkerProfileId;
  protected _name: string;
  protected apikey: string;
  protected maxWorkers: number;
  protected workerConfig: Partial<WorkerConfig>;
  protected activeWorkers: Map<WorkerId, WorkerProxy<BaseJob>>;

  constructor(
    id: WorkerProfileId,
    profileConfig: RestWorkerProfileConfig,
    protected queue: Queue,
    protected backend: Backend,
  ) {
    this._id = id;
    this._name = profileConfig.name;
    this.apikey = profileConfig.apikey;
    this.maxWorkers = profileConfig.maxWorkers ?? DefaultProfileManager.DEFAULT_CONFIG.maxWorkers;
    this.workerConfig = { ...(profileConfig.config ?? {}) };
    this.activeWorkers = new Map();
  }

  get id(): WorkerProfileId {
    return this._id;
  }

  get name(): string {
    return this._name;
  }

  get config(): Partial<WorkerConfig> {
    return this.workerConfig;
  }

  async registerNewWorkerProxy(meta: RemoteWorkerMetadata): Promise<WorkerProxy<BaseJob> | undefined> {
    const worker = await new WorkerProxy<BaseJob>(
      this,
      this.queue.options.queueNames,
      this.backend,
      this.queue,
    ).register(meta);
    if (worker.id === undefined) return undefined;
    this.activeWorkers.set(worker.id, worker);
    return worker;
  }

  async getWorkerProxy(workerId: WorkerId, meta: RemoteWorkerMetadata): Promise<WorkerProxy<BaseJob> | undefined> {
    const knownWorker = this.activeWorkers.get(workerId);
    if (knownWorker !== undefined) return knownWorker;

    const recoveredWorker = await new WorkerProxy<Job<JobArgs>>(
      this,
      this.queue.options.queueNames,
      this.backend,
      this.queue,
    ).recover(workerId, meta);
    if (recoveredWorker.id === undefined) return undefined;
    this.activeWorkers.set(recoveredWorker.id, recoveredWorker);
    return recoveredWorker;
  }

  retireWorkerProxy(workerId: WorkerId) {
    this.activeWorkers.delete(workerId);
  }

  pruneWorkerProxies(expireAfter: number): void {
    for (const [workerId, worker] of this.activeWorkers) {
      if (worker.isExpired(expireAfter)) {
        this.activeWorkers.delete(workerId);
      } else {
        worker.pruneExecutorProxies(expireAfter);
      }
    }
  }

  timingSafeApikeyCompare(apikey: string): boolean {
    return timingSafeCompare(Buffer.from(apikey), Buffer.from(this.apikey));
  }

  async atMaxCapacity(backend: WorkerBackend): Promise<boolean> {
    const options: ListWorkersOptions = {
      state: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
      metadata: [
        {
          ':profile_id': this._id,
        },
      ],
    };
    const infos = await backend.getWorkerInfos(0, 0, options);
    const activeWorkers = infos.total;
    return activeWorkers >= this.maxWorkers;
  }
}
