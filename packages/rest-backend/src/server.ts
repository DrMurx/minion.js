import bearerAuthPlugin from '@fastify/bearer-auth';
import {
  Backend,
  DefaultQueue,
  DefaultWorker,
  type InferJobArgs,
  type Job,
  type JobArgs,
  type QueueOptions,
  type WorkerConfig,
  type WorkerId,
  type WorkerRegistrationOptions,
} from '@queuebone/core';
import { type FastifyInstance } from 'fastify';
import { Executor } from './executor.js';
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
import { Worker } from './worker.js';

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
  config?: Partial<WorkerConfig>;
}

export class RestServerQueue<BaseJob extends Job<JobArgs> = Job<JobArgs>> extends DefaultQueue<BaseJob> {
  protected classes: Map<string, RemoteWorkerClass<BaseJob>> = new Map();

  constructor(
    protected fastify: FastifyInstance,
    backend: Backend,
    options: Partial<RestServerQueueOptions<BaseJob>> = {},
  ) {
    super(backend, options);

    if (options.remoteWorkerConfigs !== undefined && Array.isArray(options.remoteWorkerConfigs)) {
      options.remoteWorkerConfigs.forEach((remoteWorkerConfig) => this.addRemoteWorkerConfig(remoteWorkerConfig));
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

  addRemoteWorkerConfig(remoteWorkerConfig: RemoteWorkerClassConfig) {
    const token = remoteWorkerConfig.token;
    if (this.classes.has(token)) {
      throw new Error(`Token ${token} already exists`);
    }

    const holder: RemoteWorkerClass<BaseJob> = {
      name: remoteWorkerConfig.name,
      config: {
        ...DefaultWorker.DEFAULT_CONFIG,
        queueNames: this._options.queueNames,
        ...(remoteWorkerConfig.config ?? {}),
      },
      maxWorkers: remoteWorkerConfig.maxWorkers ?? 1,
      activeWorkers: new Map(),
    };

    this.classes.set(token, holder);
  }

  private setupRoutes() {
    type BaseJobArgs = InferJobArgs<BaseJob>;

    this.fastify.register(bearerAuthPlugin, {
      keys: [],
      auth: (key) => this.classes.has(key), // This is not timing safe!
    });

    /**
     * Register a new worker.
     */
    this.fastify.post<RegisterWorkerAPI>('/workers', { schema: registerWorkerSchema }, async (request, reply) => {
      const key = request.headers.authorization!.substring(7)!;
      const holder = this.classes.get(key);
      if (holder === undefined) {
        reply.status(401).send(); // 401 = unauthorized
        return;
      }

      if (holder.activeWorkers.size >= holder.maxWorkers) {
        reply.status(403).send(); // 403 = forbidden
        return;
      }

      const options: WorkerRegistrationOptions = {
        config: holder.config,
        metadata: {
          name: holder.name,
          host: request.ip,
        },
      };

      const worker = await new Worker<BaseJob>(this._backend, options, this).register();
      holder.activeWorkers.set(worker.id!, worker);

      return worker.workerInfo;
    });

    /**
     * Update a worker
     */
    this.fastify.patch<UpdateWorkerAPI>('/workers/:id', { schema: updateWorkerSchema }, async (request, reply) => {
      const key = request.headers.authorization!.substring(7)!;
      const holder = this.classes.get(key);
      if (holder === undefined) {
        reply.status(401).send(); // 401 = unauthorized
        return;
      }

      const { id: workerId } = request.params;
      const worker = holder.activeWorkers.get(workerId);
      if (worker === undefined) {
        reply.status(401).send(); // 401 = unauthorized
        return;
      }

      const workerInfo = await worker.update(request.body.state);
      if (workerInfo !== undefined) {
        return workerInfo;
      } else {
        reply.status(404).send(); // 404 = not found
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
        const holder = this.classes.get(key);
        if (holder === undefined) {
          reply.status(401).send(); // 401 = unauthorized
          return;
        }

        const { id: workerId } = request.params;
        const worker = holder.activeWorkers.get(workerId);
        if (worker === undefined) {
          reply.status(401).send(); // 401 = unauthorized
          return;
        }

        await worker.unregister();
        holder.activeWorkers.delete(workerId);
      },
    );

    this.fastify.post<CheckWorkerInboxAPI>(
      '/workers/:id/inbox',
      { schema: checkWorkerInboxSchema },
      async (request, reply) => {
        const key = request.headers.authorization!.substring(7)!;
        const holder = this.classes.get(key);
        if (holder === undefined) {
          reply.status(401).send(); // 401 = unauthorized
          return;
        }

        const { id: workerId } = request.params;
        const worker = holder.activeWorkers.get(workerId);
        if (worker === undefined) {
          reply.status(401).send(); // 401 = unauthorized
          return;
        }

        const commands = await worker.getInbox(request.body.state);
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
        const holder = this.classes.get(key);
        if (holder === undefined) {
          reply.status(401).send(); // 401 = unauthorized
          return;
        }

        const { id: workerId } = request.params;
        const worker = holder.activeWorkers.get(workerId);
        if (worker === undefined) {
          reply.status(401).send(); // 401 = unauthorized
          return;
        }

        const jobInfo = await worker.getNextJob(request.body.taskNames, request.body.options.minPriority ?? 0);

        if (jobInfo !== null) {
          return jobInfo;
        } else {
          reply.status(204).send(); // 204 = No content
        }
      },
    );

    this.fastify.patch<UpdateJobAPI>('/jobs/:id/:attempt', { schema: updateJobSchema }, async (request, reply) => {
      const key = request.headers.authorization!.substring(7)!;
      const holder = this.classes.get(key);
      if (holder === undefined) {
        reply.status(401).send(); // 401 = unauthorized
        return;
      }

      const { id: jobId, attempt } = request.params;
      const jobInfo = await this._backend.getJobInfo<BaseJobArgs>(jobId);
      if (
        jobInfo === undefined ||
        jobInfo.attempt !== attempt ||
        jobInfo.workerId === undefined ||
        !holder.activeWorkers.has(jobInfo.workerId)
      ) {
        reply.status(404).send(); // 404 = not found
        return;
      }

      const executor = new Executor(jobInfo, this._backend, this);

      let responseCode = 0;

      const metadata = request.body.metadata;
      if (metadata !== undefined) {
        const isUpdated = await executor.amendMetadata(metadata);
        if (!isUpdated) {
          responseCode = 404;
        }
      }

      const progress = request.body.progress;
      if (progress !== undefined) {
        const isUpdated = await executor.updateProgress(progress);
        if (!isUpdated) {
          responseCode = 404;
        }
      }

      const state = request.body.state;
      const result = request.body.result;
      if (state !== undefined && result !== undefined) {
        jobInfo.state = state;
        jobInfo.result = result;
        const isUpdated = await executor.markFinished(state, result);
        if (!isUpdated) {
          responseCode = 404;
        }
      }
      if (responseCode === 0) {
        return executor.jobInfo;
      }
      reply.status(responseCode).send();
    });
  }
}

interface RemoteWorkerClass<BaseJob extends Job<JobArgs>> {
  name: string;
  config: WorkerConfig;
  maxWorkers: number;
  activeWorkers: Map<WorkerId, Worker<BaseJob>>;
}
