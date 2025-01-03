/* eslint-disable @typescript-eslint/no-unused-vars */

import '@fastify/jwt';
import { type Job, type JobArgs } from '@queuebone/core';
import {
  type ContextConfigDefault,
  type FastifyBaseLogger,
  type FastifySchema,
  type FastifyTypeProvider,
  type FastifyTypeProviderDefault,
  type RawRequestDefaultExpression,
  type RawServerBase,
  type RawServerDefault,
  type RouteGenericInterface,
} from 'fastify';
import { type FastifyRequestType, type ResolveFastifyRequestType } from 'fastify/types/type-provider';
import { type WorkerProfileHolder, type WorkerProfileId } from './types.js';
import { type WorkerProxy } from './worker-proxy.js';

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
    profile: WorkerProfileHolder<Job<JobArgs>>;
    worker: WorkerProxy<Job<JobArgs>>;
  }
}

declare module '@fastify/jwt' {
  interface FastifyJWT {
    user: {
      prf: WorkerProfileId;
      wrk: number;
    };
  }
}
