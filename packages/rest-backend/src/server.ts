import bearerAuthPlugin from '@fastify/bearer-auth';
import {
  Backend,
  DefaultJob,
  DefaultQueue,
  DefaultWorker,
  type Job,
  type JobArgs,
  type JobInfo,
  type QueueOptions,
  type WorkerConfig,
  type WorkerId,
  type WorkerInfo,
  type WorkerRegistrationOptions,
  type WorkerUpdateOptions,
} from '@queuebone/core';
import { type FastifyInstance } from 'fastify';
import {
  type AssignNextJobAPI,
  assignNextJobSchema,
  type CheckWorkerInboxAPI,
  checkWorkerInboxSchema,
  type RegisterWorkerAPI,
  registerWorkerSchema,
  type UnregisterWorkerAPI,
  unregisterWorkerSchema,
  type UpdateJobAPI,
  updateJobSchema,
  type UpdateWorkerAPI,
  updateWorkerSchema,
} from './server-schema.js';

export interface RestServerQueueOptions<BaseJob extends Job<JobArgs>> extends QueueOptions<BaseJob> {
  remoteWorkerConfigs?: RemoteWorkerClassConfig[];
}

export interface RemoteWorkerClassConfig {
  /**
   * A name for this worker configuration.
   */
  name: string;

  /**
   * The Bearer Auth token to secure any requests.
   */
  token: string;

  /**
   * Number of workers allowed to connect at the same time with this configuration.
   */
  maxWorkers?: number;

  /**
   * Config for the corresponding worker
   */
  config: Partial<WorkerConfig>;
}

export class RestServerQueue<BaseJob extends Job<JobArgs> = DefaultJob<JobArgs>> extends DefaultQueue<BaseJob> {
  protected classes: Map<string, RemoteWorkerClass> = new Map();

  constructor(
    protected fastify: FastifyInstance,
    backend: Backend,
    options: Partial<RestServerQueueOptions<BaseJob>> = {},
  ) {
    super(backend, options);

    if (options.remoteWorkerConfigs !== undefined && Array.isArray(options.remoteWorkerConfigs)) {
      options.remoteWorkerConfigs.forEach((remoteWorkerConfig) => this.addRemoteWorker(remoteWorkerConfig));
    }

    // Plug in event listener to remove lost workers
    this.on('worker_lost', ({ workerInfo }) => {
      this.classes.forEach((holder) => {
        if (holder.activeWorkers.has(workerInfo.id)) {
          holder.activeWorkers.delete(workerInfo.id);
        }
      });
    });

    this.setupRoutes();
  }

  addRemoteWorker(remoteWorkerConfig: RemoteWorkerClassConfig) {
    const token = remoteWorkerConfig.token;
    if (this.classes.has(token)) {
      throw new Error(`Token ${token} already exists`);
    }

    const holder: RemoteWorkerClass = {
      name: remoteWorkerConfig.name,
      config: {
        ...DefaultWorker.DEFAULT_CONFIG,
        queueNames: this._options.queueNames,
        ...remoteWorkerConfig.config,
      },
      maxWorkers: remoteWorkerConfig.maxWorkers ?? 1,
      activeWorkers: new Map(),
    };

    this.classes.set(token, holder);
  }

  private setupRoutes() {
    this.fastify.register(bearerAuthPlugin, {
      keys: [],
      auth: (key) => this.classes.has(key), // This is not timing safe!
    });

    /**
     * Register a new worker.
     */
    this.fastify.post<RegisterWorkerAPI>('/workers', { schema: registerWorkerSchema }, async (request, reply) => {
      const key = request.headers.authorization!.substring(7)!;
      const holder = this.classes.get(key)!;

      if (holder.activeWorkers.size >= holder.maxWorkers) {
        reply.status(403).send();
        return;
      }

      const options: WorkerRegistrationOptions = {
        config: holder.config,
        metadata: {
          name: holder.name,
          host: request.ip,
        },
      };

      const workerInfo = await this._backend.registerWorker(options);
      if (workerInfo !== undefined) {
        holder.activeWorkers.set(workerInfo.id, workerInfo);
        this.emit('worker_registered', { workerInfo });
      }

      return workerInfo;
    });

    /**
     * Update a worker
     */
    this.fastify.patch<UpdateWorkerAPI>('/workers/:id', { schema: updateWorkerSchema }, async (request, reply) => {
      const key = request.headers.authorization!.substring(7)!;
      const holder = this.classes.get(key)!;

      const { id: workerId } = request.params;
      if (!holder.activeWorkers.has(workerId)) {
        reply.status(401).send();
        return;
      }

      const options: WorkerUpdateOptions = {
        state: request.body.state,
        finishedJobCount: 0,
      };
      const workerInfo = await this._backend.updateWorker(workerId, options);
      if (workerInfo !== undefined) {
        return workerInfo;
      } else {
        reply.status(404).send();
      }
    });

    /**
     * Delete the worker with the `:id`.
     */
    this.fastify.delete<UnregisterWorkerAPI>(
      '/workers/:id',
      { schema: unregisterWorkerSchema },
      async (request, reply) => {
        const key = request.headers.authorization!.substring(7)!;
        const holder = this.classes.get(key)!;

        const { id: workerId } = request.params;
        if (!holder.activeWorkers.has(workerId)) {
          reply.status(401).send();
          return;
        }

        const isUpdated = await this._backend.unregisterWorker(workerId);
        if (isUpdated) {
          holder.activeWorkers.delete(workerId);
          this.emit('worker_unregistered', { workerId });
          return;
        } else {
          reply.status(404).send();
        }
      },
    );

    this.fastify.post<CheckWorkerInboxAPI>(
      '/workers/:id/inbox',
      { schema: checkWorkerInboxSchema },
      async (request, reply) => {
        const key = request.headers.authorization!.substring(7)!;
        const holder = this.classes.get(key)!;

        const { id: workerId } = request.params;
        if (!holder.activeWorkers.has(workerId)) {
          reply.status(401).send();
          return;
        }

        const options: WorkerUpdateOptions = {
          state: request.body.state,
          finishedJobCount: 0,
        };
        const commands = await this._backend.checkWorkerInbox(workerId, options);
        if (commands.length !== 0) {
          return commands;
        } else {
          reply.status(204).send(); // 204 = No content
        }
      },
    );

    /**
     * Assigns the next available job to the requesting worker with the `:id`
     */
    this.fastify.post<AssignNextJobAPI>(
      '/workers/:id/nextjob',
      { schema: assignNextJobSchema },
      async (request, reply) => {
        const key = request.headers.authorization!.substring(7)!;
        const holder = this.classes.get(key)!;

        const { id: workerId } = request.params;
        if (!holder.activeWorkers.has(workerId)) {
          reply.status(401).send();
          return;
        }

        const { taskNames } = request.body;
        const { queueNames, dequeueTimeout } = holder.config;
        const minPriority = request.body.options.minPriority ?? 0;

        const jobInfo = await this._backend.assignNextJob(workerId, taskNames, dequeueTimeout, {
          queueNames,
          minPriority,
        });
        if (jobInfo !== null) {
          return jobInfo;
        } else {
          reply.status(204).send(); // 204 = No content
        }
      },
    );

    this.fastify.patch<UpdateJobAPI>('/jobs/:id/:attempt', { schema: updateJobSchema }, async (request, reply) => {
      const key = request.headers.authorization!.substring(7)!;
      const holder = this.classes.get(key)!;

      const { id: jobId, attempt } = request.params;
      const jobInfo = await this._backend.getJobInfo(jobId);
      if (jobInfo === undefined || jobInfo.attempt !== attempt || !holder.activeWorkers.has(jobInfo.workerId!)) {
        reply.status(404).send();
        return;
      }

      const partialJobInfo: Partial<JobInfo<JobArgs>> = {};
      let responseCode = 200;

      const metadata = request.body.metadata;
      if (metadata !== undefined) {
        const newMetadata = await this._backend.amendJobMetadata(jobId, attempt, metadata);
        if (newMetadata !== undefined) {
          partialJobInfo.metadata = newMetadata;
        } else {
          responseCode = 404;
        }
      }

      const progress = request.body.progress;
      if (progress !== undefined) {
        const isUpdated = await this._backend.updateJobProgress(jobId, attempt, progress);
        if (isUpdated) {
          partialJobInfo.progress = progress;
        } else {
          responseCode = 404;
        }
      }

      const state = request.body.state;
      const result = request.body.result;
      if (state !== undefined && result !== undefined) {
        const isUpdated = await this._backend.markJobFinished(jobId, attempt, state, result);
        if (isUpdated) {
          partialJobInfo.state = state;
          partialJobInfo.result = result;
        } else {
          responseCode = 404;
        }
      }
      if (responseCode === 200) {
        return partialJobInfo;
      }
      reply.status(responseCode).send();
    });
  }
}

interface RemoteWorkerClass {
  name: string;
  config: WorkerConfig;
  maxWorkers: number;
  activeWorkers: Map<WorkerId, WorkerInfo>;
}
