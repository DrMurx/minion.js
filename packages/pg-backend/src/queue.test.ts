import { type JobId, type WorkerId } from '@queuebone/core';
import { runQueueTests, type TestableBackend } from '@queuebone/core/test-suite';
import { JOB_TABLE, PgBackend, WORKER_TABLE } from './backend.js';
import { createPool } from './factory.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};
const SCHEMA = 'queue_test';

class TestablePgBackend extends PgBackend implements TestableBackend {
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

const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=${SCHEMA}`);

// Isolate tests
await pool.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
await pool.query(`CREATE SCHEMA ${SCHEMA}`);

const backend = new TestablePgBackend(pool);
await runQueueTests(backend, skip);

// Clean up once we are done
await pool.query(`DROP SCHEMA ${SCHEMA} CASCADE`);
await pool.end();
