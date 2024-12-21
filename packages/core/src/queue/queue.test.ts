import { MemoryBackend } from '../backends/memory.js';
import { runQueueTests, type TestableBackend } from '../test-suites/queue.js';
import { type JobId } from '../types/job.js';
import { type WorkerId } from '../types/worker.js';

class TestableMemoryBackend extends MemoryBackend implements TestableBackend {
  async dateBackJobsDelayUntil(jobIds: JobId[], msBeforeNow: number): Promise<void> {
    const jobs = this.selectJobs((j) => jobIds.includes(j.id));
    [...jobs].forEach((job) => (job.delayUntil = new Date(Date.now() - msBeforeNow)));
  }

  async dateBackJobExpiresAt(jobId: JobId, msBeforeNow: number): Promise<void> {
    const job = this.selectJob((j) => j.id === jobId)!;
    job.expiresAt = new Date(Date.now() - msBeforeNow);
  }

  async dateBackJobFinishedAt(jobId: JobId, ms: number): Promise<void> {
    const job = this.selectJob((j) => j.id === jobId)!;
    job.finishedAt = new Date(job.finishedAt!.getTime() - ms);
  }

  async dateBackWorkerLastseenAt(workerId: WorkerId, msBeforeNow: number): Promise<void> {
    this.selectWorker((w) => w.id === workerId)!.lastSeenAt = new Date(Date.now() - msBeforeNow);
  }
}

const backend = new TestableMemoryBackend();
await runQueueTests(backend);
