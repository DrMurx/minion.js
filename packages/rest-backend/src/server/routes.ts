/* eslint-disable @typescript-eslint/triple-slash-reference */
/// <reference path="./fastify-declare.ts" />

import jwtPlugin from '@fastify/jwt';
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
  type UpdateJobAPI,
  updateJobSchema,
  type UpdateWorkerAPI,
  updateWorkerSchema,
} from './schemas.js';
import { type ProfileManager } from './types.js';
import { WorkerProxy } from './worker-proxy.js';

export interface PluginOptions<BaseJob extends Job<JobArgs>> {
  profileManager: ProfileManager<BaseJob>;
  jwtSecret: string;
  backend: Backend;
  notifier: QueueEventEmitter<BaseJob>;
}

export const routesPlugin: FastifyPluginAsync<PluginOptions<Job<JobArgs>>> = async (fastify, options) => {
  const { profileManager, jwtSecret, backend, notifier } = options;

  fastify.register(jwtPlugin, {
    secret: jwtSecret,
  });

  /**
   * Register a new worker.
   */
  fastify.post<RegisterWorkerAPI>('/workers', { schema: registerWorkerSchema }, async ({ body, ip }, reply) => {
    // Authenticate and authorize
    const { name, passphrase } = body;
    const holder = profileManager.timingSafeGet(name, passphrase);
    if (holder === undefined) {
      return reply.status(401).send(); // 401 = unauthorized
    }

    // Ensure capacity
    if (holder.activeWorkers.size >= holder.maxWorkers) {
      return reply.status(403).send(); // 403 = forbidden
    }

    // Create and store WorkerProxy
    const worker = await new WorkerProxy<Job<JobArgs>>(name, holder.config, ip, backend, notifier).register();
    if (worker.id === undefined) {
      return reply.status(500).send(); // 500 = internal server error
    }
    holder.activeWorkers.set(worker.id, worker);
    return {
      token: fastify.jwt.sign({ name: holder.name, id: worker.id }),
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
      const profile = profileManager.get(request.user.name);
      if (profile === undefined) {
        return reply.status(401).send(); // 401 = unauthorized
      }
      const worker = profile.activeWorkers.get(request.user.id);
      if (worker === undefined) {
        return reply.status(401).send(); // 401 = unauthorized
      }

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

        const executor = new ExecutorProxy(jobRecord, worker, backend, notifier);
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
