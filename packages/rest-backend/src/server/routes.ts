/* eslint-disable @typescript-eslint/triple-slash-reference */
/// <reference path="./fastify-declare.ts" />

import jwtPlugin from '@fastify/jwt';
import { type Backend, type Job, type JobArgs, type Queue } from '@queuebone/core';
import { type FastifyPluginAsync } from 'fastify';
import { ExecutorProxy } from './executor-proxy.js';
import { Pruner } from './pruner.js';
import {
  type AssignNextJobAPI,
  assignNextJobSchema,
  type CheckWorkerInboxAPI,
  checkWorkerInboxSchema,
  type PingAPI,
  pingSchema,
  type RegisterWorkerAPI,
  registerWorkerSchema,
  type UpdateJobAPI,
  updateJobSchema,
  type UpdateWorkerAPI,
  updateWorkerSchema,
} from './schemas.js';
import { type ProfileManager } from './types.js';
import { WorkerProxy } from './worker-proxy.js';

export interface QueueboneRestServerOptions<BaseJob extends Job<JobArgs>> {
  queue: Queue<BaseJob>;
  backend: Backend;
  profileManager: ProfileManager<BaseJob>;
  jwtSecret: string;
}

export const queueboneRestServerPlugin: FastifyPluginAsync<QueueboneRestServerOptions<Job<JobArgs>>> = async (
  fastify,
  options,
) => {
  const { queue, backend, profileManager, jwtSecret } = options;

  fastify.register(jwtPlugin, {
    secret: jwtSecret,
  });

  // Setup queue
  fastify.addHook('onReady', async () => {
    await queue.start();
  });
  fastify.addHook('onClose', async () => {
    await queue.stop();
  });

  // Setup pruner
  const pruner = new Pruner(profileManager, queue.options);
  fastify.addHook('onReady', () => {
    pruner.startPruner();
  });
  fastify.addHook('onClose', () => {
    pruner.stopPruner();
  });

  /**
   * Allow for a ping, optionally checking the validity of credentials.
   */
  fastify.post<PingAPI>('/ping', { schema: pingSchema }, async ({ body }) => {
    let status = 'pong';

    // Authenticate and authorize
    const { apikey } = body;
    if (apikey !== undefined) {
      await new Promise((res) => setTimeout(res, 100));
      const profile = profileManager.timingSafeGet(apikey);
      if (profile !== undefined) {
        status = 'authenticated';
      }
    }
    return { status };
  });

  /**
   * Register a new worker.
   */
  fastify.post<RegisterWorkerAPI>('/workers', { schema: registerWorkerSchema }, async ({ body, ip }, reply) => {
    // Authenticate and authorize
    const { apikey } = body;
    const profile = profileManager.timingSafeGet(apikey);
    if (profile === undefined) {
      return reply.status(401).send(); // 401 = unauthorized
    }

    // Create WorkerProxy (fails if profile's maxWorkers are exhausted)
    const worker = await new WorkerProxy<Job<JobArgs>>(profile, queue.options.queueNames, backend, queue).register(ip);
    if (worker.id === undefined) {
      return reply.status(403).send(); // 403 = forbidden
    }

    profile.activeWorkers.set(worker.id, worker);
    return {
      token: fastify.jwt.sign({ prf: profile.id, wrk: worker.id }),
      info: worker.clientWorkerInfo,
    };
  });

  /**
   * All routes here are authenticated and set request.worker to the corresponding WorkerProxy object
   */
  fastify.register(async (fastify) => {
    fastify.decorateRequest('profile');
    fastify.decorateRequest('worker');
    fastify.addHook('preHandler', async (request, reply) => {
      // Verify JWT
      try {
        await request.jwtVerify();
      } catch (err) {
        return reply.status(401).send(err);
      }

      // Obtain proper WorkerProxy from profile holder
      const profile = profileManager.get(request.user.prf);
      if (profile === undefined) {
        return reply.status(401).send(); // 401 = unauthorized
      }
      const worker = profile.activeWorkers.get(request.user.wrk);
      if (worker === undefined) {
        return reply.status(401).send(); // 401 = unauthorized
      }
      worker.lastSeenAt = Date.now();

      request.profile = profile;
      request.worker = worker;
    });

    /**
     * Update a worker
     */
    fastify.patch<UpdateWorkerAPI>('/worker', { schema: updateWorkerSchema }, async ({ worker, body }) => {
      await worker.setState(body.state);
      return worker.clientWorkerInfo;
    });

    /**
     * Delete the worker with the `:id`.
     */
    fastify.delete('/worker', { schema: {} }, async ({ worker, profile }) => {
      if (worker.id !== undefined) {
        profile.activeWorkers.delete(worker.id);
      }
      await worker.unregister();
    });

    /**
     * Query the worker's inbox.
     */
    fastify.post<CheckWorkerInboxAPI>(
      '/worker/inbox',
      { schema: checkWorkerInboxSchema },
      async ({ worker, body }, reply) => {
        const commands = await worker.getInbox(body.state);
        return commands.length !== 0 ? commands : reply.status(204).send(); // 204 = No content
      },
    );

    /**
     * Assigns the next available job to the requesting worker with the `:id`
     */
    fastify.post<AssignNextJobAPI>(
      '/worker/nextjob',
      { schema: assignNextJobSchema },
      async ({ worker, body }, reply) => {
        const { taskNames, options } = body;
        const jobRecord = await worker.getNextJob(taskNames, options.minPriority ?? 0);

        if (jobRecord === null) {
          return reply.status(204).send(); // 204 = No content
        }

        const executor = new ExecutorProxy(jobRecord, worker, backend, queue);
        await executor.start();
        worker.jobExecutors.set(jobRecord.id, executor);

        return jobRecord;
      },
    );

    fastify.patch<UpdateJobAPI>(
      '/jobs/:id/:attempt',
      { schema: updateJobSchema },
      async ({ worker, params, body }, reply) => {
        const { id, attempt } = params;

        const executor = worker.jobExecutors.get(id);
        if (executor === undefined || executor.attempt !== attempt) {
          return reply.status(404).send(); // 404 = not found
        }
        executor.lastSeenAt = Date.now();

        const { metadata, progress, state, result } = body;
        if (metadata !== undefined) {
          const isUpdated = await executor.amendMetadata(metadata);
          return isUpdated ? executor.jobRecord : reply.status(404).send();
        }
        if (progress !== undefined) {
          const isUpdated = await executor.updateProgress(progress);
          return isUpdated ? executor.jobRecord : reply.status(404).send();
        }
        if (state !== undefined && result !== undefined) {
          const isUpdated = await executor.markFinished(state, result);
          worker.jobExecutors.delete(id);
          return isUpdated ? executor.jobRecord : reply.status(404).send();
        }
        return reply.status(404).send();
      },
    );
  });
};
