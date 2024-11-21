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
import { type WorkerProfileHolder } from './types.ts';
import { type WorkerProxy } from './worker-proxy.ts';

/* eslint-disable @typescript-eslint/no-unused-vars */
declare module 'fastify' {
  interface FastifyRequest<
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
