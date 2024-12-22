import { runWorkerTests } from '@queuebone/core/test-suite';
import t from 'tap';
import { PgBackend } from './backend.js';
import { runTestsWithPgContainer } from './test-suite/container-runner.js';

await runTestsWithPgContainer(t, PgBackend, runWorkerTests);
