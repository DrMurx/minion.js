import { type JobDequeueOptions } from '../types/backend.js';
import { Queue } from '../types/queue.js';

export class QuickRunner {
  constructor(protected queue: Queue) {}

  /**
   * Retry job in a foreground queue, then perform it right away with a temporary worker in this process,
   * very useful for debugging.
   */
  async runJob(jobId: number): Promise<boolean> {
    const queueName = this.queue.FOREGROUND_QUEUE;
    const jobCtrl = await this.queue.getJob(jobId);
    if (jobCtrl === null) return false;
    if ((await jobCtrl.retry({ queueName, maxAttempts: jobCtrl.maxAttempts + 1 })) === null) return false;

    const worker = await this.queue.getNewWorker({ queueNames: [queueName] }).register();
    try {
      const executor = await worker.getNextExecutor(0, { id: jobId });
      if (executor === null) return false;
      await executor.perform(true);
      return true;
    } finally {
      await worker.unregister();
    }
  }

  /**
   * Perform all jobs with a temporary worker, very useful for testing.
   */
  async runJobs(options?: JobDequeueOptions): Promise<void> {
    const worker = await this.queue.getNewWorker().register();
    try {
      while (true) {
        await worker.heartbeat();
        const executor = await worker.getNextExecutor(0, options);
        if (executor === null) break;
        await executor.perform();
      }
    } finally {
      await worker.unregister();
    }
  }
}
