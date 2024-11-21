import {
  Backend,
  DefaultQueue,
  DefaultWorker,
  type Job,
  type JobArgs,
  type WorkerConfig,
  type WorkerId,
} from '@queuebone/core';
import { type FastifyInstance } from 'fastify';
import { type ServerQueueOptions, type WorkerProfile } from './config.js';
import { routes } from './routes.js';
import { type WorkerProxy } from './worker-proxy.js';

export interface WorkerProfileHolder<BaseJob extends Job<JobArgs>> {
  name: string;
  token: string;
  maxWorkers: number;
  config: WorkerConfig;
  activeWorkers: Map<WorkerId, WorkerProxy<BaseJob>>;
}

export class ServerQueue<BaseJob extends Job<JobArgs> = Job<JobArgs>> extends DefaultQueue<BaseJob> {
  protected _holders: Map<string, WorkerProfileHolder<BaseJob>> = new Map();

  constructor(
    protected fastify: FastifyInstance,
    backend: Backend,
    options: Partial<ServerQueueOptions<BaseJob>> = {},
  ) {
    super(backend, options);

    if (Array.isArray(options.workerProfiles)) {
      options.workerProfiles.forEach((profile) => this.addWorkerProfile(profile));
    }

    // Plug in event listener to remove lost workers
    this.on('worker_lost', ({ workerInfo }) => {
      this._holders.forEach((holder) => {
        if (holder.activeWorkers.has(workerInfo.id)) {
          holder.activeWorkers.delete(workerInfo.id);
        }
      });
    });

    // Register routes
    this.fastify.register(routes, {
      holders: this._holders,
      backend: this._backend,
      notifier: this,
    });
  }

  addWorkerProfile(profileConfig: WorkerProfile) {
    const token = profileConfig.token;
    if (this._holders.has(token)) {
      throw new Error(`Token ${token} already exists`);
    }

    const holder: WorkerProfileHolder<BaseJob> = {
      ...profileConfig,
      config: {
        ...DefaultWorker.DEFAULT_CONFIG,
        queueNames: this._options.queueNames,
        ...(profileConfig.config ?? {}),
      },
      maxWorkers: profileConfig.maxWorkers ?? 1,
      activeWorkers: new Map(),
    };

    this._holders.set(token, holder);
  }
}
