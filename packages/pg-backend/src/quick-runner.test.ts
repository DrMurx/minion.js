import { runQuickRunnerTests, type Backend } from '@queuebone/core';
import { PgBackend } from './backend.js';
import { createPool } from './factory.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};
const SCHEMA = 'queue_quickrunner_test';

const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=${SCHEMA}`);

// Isolate tests
await pool.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
await pool.query(`CREATE SCHEMA ${SCHEMA}`);

const backend: Backend = new PgBackend(pool);
await runQuickRunnerTests(backend, skip);

// Clean up once we are done
await pool.query(`DROP SCHEMA ${SCHEMA} CASCADE`);
await pool.end();
