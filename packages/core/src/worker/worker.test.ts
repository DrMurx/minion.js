import { MemoryBackend } from '../backends/memory.js';
import { runWorkerTests } from '../test-suites/worker.js';

const backend = new MemoryBackend();
await runWorkerTests(backend);
