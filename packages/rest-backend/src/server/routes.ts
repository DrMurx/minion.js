import { type Backend, type Job, type JobArgs, type QueueEventEmitter } from '@queuebone/core';
import { type FastifyPluginAsync } from 'fastify';
import { ExecutorProxy } from './executor-proxy.js';
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
  type WorkerAPI,
} from './schemas.js';
import { type ProfileManager } from './types.js';
import { WorkerProxy } from './worker-proxy.js';

export interface PluginOptions<BaseJob extends Job<JobArgs>> {
  profileManager: ProfileManager<BaseJob>;
  backend: Backend;
  notifier: QueueEventEmitter<BaseJob>;
}

export const routesPlugin: FastifyPluginAsync<PluginOptions<Job<JobArgs>>> = async (fastify, options) => {
  const { profileManager, backend, notifier } = options;

  // Plug in event listener to remove lost workers
  notifier.on('worker_lost', ({ workerInfo }) => {
    profileManager.dropWorker(workerInfo.id);
  });

  fastify.decorateRequest('holder');
  fastify.addHook('preHandler', async (request, reply) => {
    const authorization = request.headers.authorization;
    if (authorization === undefined || !authorization.startsWith('Bearer ')) {
      return reply.status(401).send(); // 401 = unauthorized
    }

    const token = authorization.substring(7)!;
    const holder = profileManager.timingSafeGet(token)!;
    if (holder === undefined) {
      return reply.status(401).send(); // 401 = unauthorized
    }

    request.holder = holder;
  });

  /**
   * Register a new worker.
   */
  fastify.post<RegisterWorkerAPI>('/workers', { schema: registerWorkerSchema }, async (request, reply) => {
    const holder = request.holder;
    if (holder.activeWorkers.size >= holder.maxWorkers) {
      return reply.status(403).send(); // 403 = forbidden
    }
    const serverWorker = await new WorkerProxy<Job<JobArgs>>(backend, holder, request.ip, notifier).register();
    return serverWorker.clientWorkerInfo;
  });

  // All /worker/:id routes
  fastify.register(
    async (fastify) => {
      fastify.decorateRequest('worker');
      fastify.addHook<WorkerAPI>('preHandler', async (request, reply) => {
        const holder = request.holder;
        const { id } = request.params;
        const worker = holder.activeWorkers.get(id);
        if (worker === undefined) {
          return reply.status(401).send(); // 401 = unauthorized
        }
        request.worker = worker;
      });

      /**
       * Update a worker
       */
      fastify.patch<UpdateWorkerAPI>('', { schema: updateWorkerSchema }, async (request) => {
        request.worker.state = request.body.state;
        await request.worker.heartbeat(true);
        return request.worker.clientWorkerInfo;
      });

      /**
       * Delete the worker with the `:id`.
       */
      fastify.delete<UnregisterWorkerAPI>('', { schema: unregisterWorkerSchema }, async (request) => {
        await request.worker.unregister();
      });

      /**
       * Query the worker's inbox.
       */
      fastify.post<CheckWorkerInboxAPI>('/inbox', { schema: checkWorkerInboxSchema }, async (request, reply) => {
        request.worker.state = request.body.state;
        const commands = await request.worker.getInbox();
        return commands.length !== 0 ? commands : reply.status(204).send(); // 204 = No content
      });

      /**
       * Assigns the next available job to the requesting worker with the `:id`
       */
      fastify.post<AssignNextJobAPI>('/nextjob', { schema: assignNextJobSchema }, async (request, reply) => {
        const { taskNames, options } = request.body;
        const jobRecord = await request.worker.getNextJob(taskNames, options.minPriority ?? 0);
        return jobRecord !== null ? jobRecord : reply.status(204).send(); // 204 = No content
      });
    },
    { prefix: '/workers/:id' },
  );

  fastify.patch<UpdateJobAPI>('/jobs/:id/:attempt', { schema: updateJobSchema }, async (request, reply) => {
    const holder = request.holder;

    const { id: jobId, attempt } = request.params;
    const jobRecord = await backend.getJobInfo<JobArgs>(jobId);
    if (jobRecord === undefined || jobRecord.attempt !== attempt || jobRecord.workerId === undefined) {
      return reply.status(404).send(); // 404 = not found
    }
    const worker = holder.activeWorkers.get(jobRecord.workerId);
    if (worker === undefined) {
      return reply.status(404).send(); // 404 = not found
    }

    const executor = new ExecutorProxy(jobRecord, worker, backend, notifier);

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
      jobRecord.state = state;
      jobRecord.result = result;
      const isUpdated = await executor.markFinished(state, result);
      if (!isUpdated) {
        responseCode = 404;
      }
    }
    if (responseCode === 0) {
      return executor.jobRecord;
    }
    return reply.status(responseCode).send();
  });
};
