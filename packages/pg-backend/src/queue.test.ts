import { runQueueTests } from '@queuebone/core/test-suite';
import t from 'tap';
import { runTestsWithPgContainer } from './test-suite/container-runner.js';
import { TestablePgBackend } from './test-suite/testable-backend.js';

await runTestsWithPgContainer(t, TestablePgBackend, runQueueTests);
