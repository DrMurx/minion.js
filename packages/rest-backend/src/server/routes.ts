/* eslint-disable @typescript-eslint/triple-slash-reference */
/// <reference path="./fastify-declare.ts" />

import jwtPlugin from '@fastify/jwt';
import { type Backend, type Job, type JobArgs, type Queue } from '@queuebone/core';
import { HttpStatusCode } from 'axios';
import { type FastifyPluginAsync } from 'fastify';
import { type RestWorkerProfileConfig } from './config.js';
import { DefaultProfileManager } from './profile-manager.js';
import { Pruner } from './pruner.js';
import {
  type AssignNextJobAPI,
  assignNextJobSchema,
  type CheckWorkerInboxAPI,
  checkWorkerInboxSchema,
  type PingAPI,
  pingSchema,
  type RegisteredWorkerAPI,
  type RegisterWorkerAPI,
  registerWorkerSchema,
  type UpdateJobAPI,
  updateJobSchema,
  type UpdateWorkerAPI,
  updateWorkerSchema,
} from './schemas.js';

export interface QueueboneRestServerOptions<BaseJob extends Job<JobArgs>> {
  queue: Queue<BaseJob>;
  backend: Backend;
  profileConfigs: RestWorkerProfileConfig[];
  jwtSecret: string;
}

export const queueboneRestServerPlugin: FastifyPluginAsync<QueueboneRestServerOptions<Job<JobArgs>>> = async (
  fastify,
  options,
) => {
  const { queue, backend, profileConfigs, jwtSecret } = options;

  const profileManager = new DefaultProfileManager(queue, backend, profileConfigs);

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
  fastify.post<RegisterWorkerAPI>('/workers', { schema: registerWorkerSchema }, async (request, reply) => {
    const { headers, body, ip } = request;

    // Authenticate and authorize
    const { apikey } = body;
    const profile = profileManager.timingSafeGet(apikey);
    if (profile === undefined) {
      return reply.status(HttpStatusCode.Unauthorized).send();
    }

    // Create WorkerProxy (fails if profile's maxWorkers are exhausted)
    const worker = await profile.registerNewWorkerProxy({
      ip,
      hostname: headers['x-hostname'],
      pid: headers['x-pid'],
    });
    if (worker === undefined) {
      return reply.status(HttpStatusCode.ServiceUnavailable).send();
    }

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
    fastify.addHook<RegisteredWorkerAPI>('preHandler', async (request, reply) => {
      const { headers, ip } = request;

      // Verify JWT
      try {
        await request.jwtVerify();
      } catch (err) {
        return reply.status(HttpStatusCode.Unauthorized).send(err);
      }

      // Obtain proper WorkerProxy from profile holder
      const profile = profileManager.get(request.user.prf);
      if (profile === undefined) {
        return reply.status(HttpStatusCode.Gone).send();
      }
      const worker = await profile.getWorkerProxy(request.user.wrk, {
        ip,
        hostname: headers['x-hostname'],
        pid: headers['x-pid'],
      });
      if (worker === undefined) {
        return reply.status(HttpStatusCode.Forbidden).send();
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
        profile.retireWorkerProxy(worker.id);
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
        return commands.length !== 0 ? commands : reply.status(HttpStatusCode.NoContent).send();
      },
    );

    /**
     * Assigns the next available job to the requesting worker. The `serial` number of the client's request
     * can be used to catch retries due to network issues.
     */
    fastify.post<AssignNextJobAPI>(
      '/worker/nextjob',
      { schema: assignNextJobSchema },
      async ({ worker, body }, reply) => {
        let errString = 'ERROR';
        try {
          const { taskNames, options, serial } = body;
          const executor = await worker.getNextExecutor(taskNames, options.minPriority ?? 0, serial);

          if (executor === null) {
            errString = `${errString} job=NULL`;
            return reply.status(HttpStatusCode.NoContent).send();
          }

          errString = `${errString} job=${executor.jobRecord.id}`;

          return executor.jobRecord;
        } catch (e: any) {
          console.error(`${errString} code=${e.code} msg=${e.message}`);
          return reply.status(HttpStatusCode.InternalServerError).send();
        }
      },
    );

    fastify.patch<UpdateJobAPI>(
      '/jobs/:id/:attempt',
      { schema: updateJobSchema },
      async ({ worker, params, body }, reply) => {
        const { id, attempt } = params;

        const executor = await worker.getRunningExecutor(id, attempt);
        if (executor === undefined) {
          return reply.status(HttpStatusCode.NotFound).send();
        }

        const { metadata, progress, state, result } = body;
        if (metadata !== undefined) {
          const jobRecord = await executor.amendMetadata(metadata);
          return jobRecord !== undefined ? jobRecord : reply.status(404).send();
        }
        if (progress !== undefined) {
          const jobRecord = await executor.updateProgress(progress);
          return jobRecord !== undefined ? jobRecord : reply.status(404).send();
        }
        if (state !== undefined && result !== undefined) {
          const jobRecord = await executor.markFinished(state, result);
          worker.finishExecutor(executor);
          return jobRecord !== undefined ? jobRecord : reply.status(404).send();
        }
        return reply.status(HttpStatusCode.NotFound).send();
      },
    );
  });
};
