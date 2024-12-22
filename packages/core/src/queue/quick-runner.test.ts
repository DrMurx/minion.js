import t from 'tap';
import { MemoryBackend } from '../backends/memory.js';
import { runQuickRunnerTests } from '../test-suites/quick-runner.js';

const backend = new MemoryBackend();
await runQuickRunnerTests(t, backend);
