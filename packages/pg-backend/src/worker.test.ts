import { runWorkerTests, type Backend } from '@queuebone/core';
import { PgBackend } from './backend.js';
import { createPool } from './factory.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};

const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=queue_worker_test`);

// Isolate tests
await pool.query('DROP SCHEMA IF EXISTS queue_worker_test CASCADE');
await pool.query('CREATE SCHEMA queue_worker_test');

const backend: Backend = new PgBackend(pool);
await runWorkerTests(backend, skip);

// Clean up once we are done
await pool.query('DROP SCHEMA queue_worker_test CASCADE');
await pool.end();
