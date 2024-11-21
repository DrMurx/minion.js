import bearerAuthPlugin from '@fastify/bearer-auth';
import { type Backend, type Job, type JobArgs, type QueueEventEmitter } from '@queuebone/core';
import {
  type ContextConfigDefault,
  type FastifyBaseLogger,
  type FastifyPluginAsync,
  type FastifySchema,
  type FastifyTypeProvider,
  type FastifyTypeProviderDefault,
  type RawRequestDefaultExpression,
  type RawServerBase,
  type RawServerDefault,
  type RouteGenericInterface,
} from 'fastify';
import { type FastifyRequestType, type ResolveFastifyRequestType } from 'fastify/types/type-provider';
import { ExecutorProxy } from './executor-proxy.ts';
import { type WorkerProfileHolder } from './queue.ts';
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
} from './server-schema.ts';
import { WorkerProxy } from './worker-proxy.ts';

/* eslint-disable @typescript-eslint/no-unused-vars */
declare module 'fastify' {
  export interface FastifyRequest<
    RouteGeneric extends RouteGenericInterface = RouteGenericInterface,
    RawServer extends RawServerBase = RawServerDefault,
    RawRequest extends RawRequestDefaultExpression<RawServer> = RawRequestDefaultExpression<RawServer>,
    SchemaCompiler extends FastifySchema = FastifySchema,
    TypeProvider extends FastifyTypeProvider = FastifyTypeProviderDefault,
    ContextConfig = ContextConfigDefault,
    Logger extends FastifyBaseLogger = FastifyBaseLogger,
    RequestType extends FastifyRequestType = ResolveFastifyRequestType<TypeProvider, SchemaCompiler, RouteGeneric>,
  > {
    holder: WorkerProfileHolder<Job<JobArgs>>;
    worker: WorkerProxy<Job<JobArgs>>;
  }
}
/* eslint-enable @typescript-eslint/no-unused-vars */

interface ServerRouteOptions<BaseJob extends Job<JobArgs>> {
  holders: Map<string, WorkerProfileHolder<BaseJob>>;
  backend: Backend;
  notifier: QueueEventEmitter<BaseJob>;
}

export const routes: FastifyPluginAsync<ServerRouteOptions<Job<JobArgs>>> = async (fastify, options) => {
  const { holders, backend, notifier } = options;
  fastify.register(bearerAuthPlugin, {
    keys: [],
    auth: (key) => holders.has(key), // This is not timing safe!
  });

  fastify.decorateRequest('holder');

  fastify.addHook('preHandler', async (request, reply) => {
    const authorization = request.headers.authorization;
    if (authorization === undefined || !authorization.startsWith('Bearer ')) {
      return reply.status(401).send();
    }

    const key = authorization.substring(7)!;
    const holder = holders.get(key);
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
      fastify.patch<UpdateWorkerAPI>('', { schema: updateWorkerSchema }, async (request, reply) => {
        const workerInfo = await request.worker.update(request.body.state);
        return workerInfo !== undefined ? workerInfo : reply.status(404).send(); // 404 = not found
      });

      /**
       * Delete the worker with the `:id`.
       */
      fastify.delete<UnregisterWorkerAPI>('', { schema: unregisterWorkerSchema }, async (request) => {
        await request.worker.unregister();
      });

      fastify.post<CheckWorkerInboxAPI>('/inbox', { schema: checkWorkerInboxSchema }, async (request, reply) => {
        const commands = await request.worker.getInbox(request.body.state);
        return commands.length !== 0 ? commands : reply.status(204).send(); // 204 = No content
      });

      /**
       * Assigns the next available job to the requesting worker with the `:id`
       */
      fastify.post<AssignNextJobAPI>('/nextjob', { schema: assignNextJobSchema }, async (request, reply) => {
        const { taskNames, options } = request.body;
        const jobInfo = await request.worker.getNextJob(taskNames, options.minPriority ?? 0);
        return jobInfo !== null ? jobInfo : reply.status(204).send(); // 204 = No content
      });
    },
    { prefix: '/workers/:id' },
  );

  fastify.patch<UpdateJobAPI>('/jobs/:id/:attempt', { schema: updateJobSchema }, async (request, reply) => {
    const holder = request.holder;

    const { id: jobId, attempt } = request.params;
    const jobInfo = await backend.getJobInfo<JobArgs>(jobId);
    if (
      jobInfo === undefined ||
      jobInfo.attempt !== attempt ||
      jobInfo.workerId === undefined ||
      !holder.activeWorkers.has(jobInfo.workerId)
    ) {
      return reply.status(404).send(); // 404 = not found
    }

    const executor = new ExecutorProxy(jobInfo, backend, notifier);

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
    return reply.status(responseCode).send();
  });
};
