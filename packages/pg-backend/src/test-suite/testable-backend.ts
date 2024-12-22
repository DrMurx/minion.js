import { type JobId, type WorkerId } from '@queuebone/core';
import { type TestableBackend } from '@queuebone/core/test-suite';
import { JOB_TABLE, PgBackend, WORKER_TABLE } from '../backend.js';

export class TestablePgBackend extends PgBackend implements TestableBackend {
  async dateBackJobsDelayUntil(jobIds: JobId[], msBeforeNow: number): Promise<void> {
    await this.pool.query(
      `UPDATE ${JOB_TABLE} SET delay_until = NOW() - $1 * INTERVAL '1 millisecond' WHERE id = ANY ($2)`,
      [msBeforeNow, jobIds],
    );
  }

  async dateBackJobExpiresAt(jobId: JobId, msBeforeNow: number): Promise<void> {
    await this.pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - $1 * INTERVAL '1 millisecond' WHERE id = $2`, [
      msBeforeNow,
      jobId,
    ]);
  }

  async dateBackJobFinishedAt(jobId: JobId, ms: number): Promise<void> {
    await this.pool.query(
      `UPDATE ${JOB_TABLE} SET finished_at = finished_at - $1 * INTERVAL '1 millisecond' WHERE id = $2`,
      [ms, jobId],
    );
  }

  async dateBackWorkerLastseenAt(workerId: WorkerId, msBeforeNow: number): Promise<void> {
    await this.pool.query(
      `UPDATE ${WORKER_TABLE} SET last_seen_at = NOW() - $1 * INTERVAL '1 millisecond' WHERE id = $2`,
      [msBeforeNow, workerId],
    );
  }
}
