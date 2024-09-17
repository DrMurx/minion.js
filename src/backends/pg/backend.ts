import os from 'node:os';
import pg from 'pg';
import { Migration, MigrationStep } from './migration.js';
import { defaultBackoffStrategy } from '../../minion.js';
import type {
  DailyHistory,
  DequeueOptions,
  DequeuedJob,
  EnqueueOptions,
  JobInfo,
  JobList,
  ListJobsOptions,
  ListWorkersOptions,
  MinionArgs,
  MinionBackend,
  MinionBackoffStrategy,
  MinionHistory,
  MinionJobId,
  MinionStats,
  MinionWorkerId,
  RegisterWorkerOptions,
  RepairOptions,
  ResetOptions,
  RetryOptions,
  WorkerInfo,
  WorkerList
} from '../../types.js';
import { createPool } from './factory.js';


/**
 * Minion PostgreSQL backend class.
 */
export class PgBackend implements MinionBackend {
  /**
   * Backend name.
   */
  public readonly name = 'Pg';

  private hostname = os.hostname();

  private _pool: pg.Pool;
  private autoclosePool = false;

  constructor(config: string | pg.Pool, private calcBackoff: MinionBackoffStrategy = defaultBackoffStrategy) {
    if (config instanceof pg.Pool) {
      pg.types.setTypeParser(20, parseInt);
      this._pool = config;
    } else {
      this._pool = createPool(config);
      this.autoclosePool = true;
    }
  }

  get pool(): pg.Pool {
    return this._pool;
  }


  /**
   * Enqueue a new job with `pending` state.
   */
  async addJob(taskName: string, args: MinionArgs = [], options: EnqueueOptions = {}): Promise<MinionJobId> {
    const results = await this._pool.query<EnqueueResult>(
      `
        INSERT INTO minion_jobs (
          queue_name,
          task_name,
          args,

          priority,
          attempts,

          parent_job_ids,
          lax_dependency,

          notes,

          delay_until,
          expires_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
          (NOW() + (INTERVAL '1 millisecond' * $10)),
          CASE WHEN $9::BIGINT IS NOT NULL THEN NOW() + (INTERVAL '1 millisecond' * $9::BIGINT) END
        )
        RETURNING id
      `,
      [
        options.queueName ?? 'default',
        taskName,
        JSON.stringify(args),
        options.priority ?? 0,
        options.attempts ?? 1,
        options.parentJobIds ?? [],
        options.laxDependency ?? false,
        options.notes ?? {},
        options.expiresAt,
        options.delayUntil ?? 0
      ]
    );

    return results.rows[0].id;
  }

  /**
   * Wait a given amount of time in milliseconds for a job, dequeue it and transition from `pending` to `running`
   * state, or return `null` if queues were empty.
   */
  async getNextJob(id: MinionWorkerId, taskNames: string[], wait: number, options: DequeueOptions): Promise<DequeuedJob | null> {
    const job = await this.tryGetNextJob(id, taskNames, options);
    if (job !== null) return job;

    const conn = await this._pool.connect();
    try {
      await conn.query('LISTEN "minion.job"');
      let timer;
      await Promise.race([
        new Promise(resolve => conn.on('notification', resolve)),
        new Promise(resolve => (timer = setTimeout(resolve, wait)))
      ]);
      clearTimeout(timer);
    } finally {
      conn.removeAllListeners('notification');
      await conn.query('UNLISTEN "minion.job"');
      conn.release();
    }

    return await this.tryGetNextJob(id, taskNames, options);
  }

  /**
   * Transition from `running` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `pending` with a delay.
   */
  async markJobFailed(id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    return await this.updateJobAfterRun('failed', id, retries, result);
  }

  /**
   * Transition from `running` to `succeeded` state with or without a result.
   */
  async markJobSucceeded(id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    return await this.updateJobAfterRun('succeeded', id, retries, result);
  }

  /**
   * Transition job back to `pending` state, already `pending` jobs may also be retried to change options.
   */
  async retryJob(id: MinionJobId, retries: number, options: RetryOptions = {}): Promise<boolean> {
    const results = await this._pool.query(
      `
        UPDATE minion_jobs SET
          attempts = COALESCE($1, attempts),
          delay_until = (NOW() + (INTERVAL '1 millisecond' * $2)),
          expires_at = CASE WHEN $3::BIGINT IS NOT NULL THEN
              NOW() + (INTERVAL '1 millisecond' * $3::BIGINT)
            ELSE
              expires_at
            END,
          lax_dependency = COALESCE($4, lax_dependency),
          parent_job_ids = COALESCE($5, parent_job_ids),
          priority = COALESCE($6, priority),
          queue_name = COALESCE($7, queue_name),
          retried_at = NOW(),
          retries = retries + 1,
          state = 'pending'
        WHERE id = $8 AND retries = $9
      `,
      [options.attempts, options.delayUntil ?? 0, options.expireAt, options.laxDependency, options.parentJobIds, options.priority, options.queueName, id, retries]
    );

    return (results.rowCount ?? 0) > 0 ? true : false;
  }

  /**
   * Remove `failed`, `succeeded` or `pending` job from queue.
   */
  async removeJob(id: MinionJobId): Promise<boolean> {
    const results = await this._pool.query(
      "DELETE FROM minion_jobs WHERE id = $1 AND state IN ('pending', 'failed', 'succeeded')",
      [id]
    );
    return (results.rowCount ?? 0) > 0;
  }

  /**
   * Returns the information about jobs in batches.
   */
  async getJobInfos(offset: number, limit: number, options: ListJobsOptions = {}): Promise<JobList> {
    const results = await this._pool.query<ListJobsResult>(
      `
        SELECT
          id,

          task_name AS "taskName",
          queue_name AS "queueName",
          args,
          result,

          state,
          priority,
          attempts,
          retries,

          parent_job_ids AS "parentJobIds",
          ARRAY(SELECT id FROM minion_jobs WHERE parent_job_ids @> ARRAY[j.id]) AS "childJobIds",
          lax_dependency AS "laxDependency",

          worker_id AS "workerId",
          notes,

          delay_until AS "delayUntil",
          started_at AS "startedAt",
          retried_at AS "retriedAt",
          finished_at AS "finishedAt",

          created_at AS "createdAt",
          expires_at AS "expiresAt",

          NOW() AS "time",
          COUNT(*) OVER() AS "total"
        FROM minion_jobs AS j
        WHERE (id < $1 OR $1 IS NULL)
          AND (id = ANY ($2) OR $2 IS NULL)
          AND (queue_name = ANY ($3) OR $3 IS NULL)
          AND (task_name = ANY ($4) OR $4 IS NULL)
          AND (state = ANY ($5) OR $5 IS NULL)
          AND (notes ? ANY ($6) OR $6 IS NULL)
          AND (state != 'pending' OR expires_at IS NULL OR expires_at > NOW())
        ORDER BY id DESC
        LIMIT $7 OFFSET $8
      `,
      [
        options.beforeId,
        options.ids,
        options.queueNames,
        options.taskNames,
        options.states,
        options.notes,
        limit,
        offset
      ]
    );
    const total = removeTotal(results.rows);
    return {total, jobs: results.rows};
  }

  /**
   * Get history information for job queue.
   */
  async getJobHistory(): Promise<MinionHistory> {
    const results = await this._pool.query<DailyHistory>(`
      SELECT
        EXTRACT(EPOCH FROM ts) AS epoch,
        COALESCE(failed_jobs, 0) AS "failedJobs",
        COALESCE(succeeded_jobs, 0) AS "succeededJobs"
      FROM (
        SELECT
          EXTRACT(DAY FROM finished_at) AS day,
          EXTRACT(HOUR FROM finished_at) AS hour,
          COUNT(*) FILTER (WHERE state = 'failed') AS failed_jobs,
          COUNT(*) FILTER (WHERE state = 'succeeded') AS succeeded_jobs
        FROM minion_jobs
        WHERE finished_at > NOW() - INTERVAL '23 hours'
        GROUP BY day, hour
      ) AS j RIGHT OUTER JOIN (
        SELECT *
        FROM GENERATE_SERIES(NOW() - INTERVAL '23 hour', NOW(), '1 hour') AS ts
      ) AS s ON EXTRACT(HOUR FROM ts) = j.hour AND EXTRACT(DAY FROM ts) = j.day
      ORDER BY epoch ASC
    `);
    return {daily: results.rows};
  }

  /**
   * Change one or more metadata fields for a job. Setting a value to `null` will remove the field.
   */
  async addNotes(id: MinionJobId, notes: Record<string, any>): Promise<boolean> {
    const results = await this._pool.query(
      'UPDATE minion_jobs SET notes = JSONB_STRIP_NULLS(notes || $1) WHERE id = $2', [notes, id]);
    return (results.rowCount ?? 0) > 0;
  }


  /**
   * Register worker or send heartbeat to show that this worker is still alive.
   */
  async registerWorker(id?: MinionWorkerId, options: RegisterWorkerOptions = {}): Promise<MinionWorkerId> {
    const status = options.status ?? {};
    const results = await this._pool.query<RegisterWorkerResult>(`
      INSERT INTO minion_workers (id, host, pid, status)
        VALUES (COALESCE($1, NEXTVAL('minion_workers_id_seq')), $2, $3, $4)
        ON CONFLICT(id) DO UPDATE SET last_seen_at = NOW(), status = $4
        RETURNING id
    `, [id, this.hostname, process.pid, status]);
    return results.rows[0].id;
  }

  /**
   * Unregister worker.
   */
  async unregisterWorker(id: MinionWorkerId): Promise<void> {
    await this._pool.query('DELETE FROM minion_workers WHERE id = $1', [id]);
  }

  /**
   * Returns information about workers in batches.
   */
  async getWorkers(offset: number, limit: number, options: ListWorkersOptions = {}): Promise<WorkerList> {
    const results = await this._pool.query<ListWorkersResult>(
      `
        SELECT
          id,

          host,
          pid,

          status,
          ARRAY(
            SELECT id FROM minion_jobs WHERE state = 'running' AND worker_id = minion_workers.id
          ) AS "jobs",

          started_at AS "startedAt",
          last_seen_at AS "lastSeenAt",

          COUNT(*) OVER() AS "total"
        FROM minion_workers
        WHERE (id < $1 OR $1 IS NULL) AND (id = ANY ($2) OR $2 IS NULL)
        ORDER BY id DESC
        LIMIT $3 OFFSET $4
      `,
      [
        options.beforeId,
        options.ids,
        limit,
        offset
      ]
    );
    const total = removeTotal(results.rows);
    return {total, workers: results.rows};
  }

  /**
   * Broadcast remote control command to one or more workers.
   */
  async notifyWorkers(command: string, args: any[] = [], ids: MinionWorkerId[] = []): Promise<boolean> {
    const results = await this._pool.query(
      `
        UPDATE minion_workers SET inbox = inbox || $1::JSONB
        WHERE (id = ANY ($2) OR $2 = '{}')
      `,
      [JSON.stringify([[command, ...args]]), ids]
    );
    return (results.rowCount ?? 0) > 0;
  }

  /**
   * Receive remote control commands for worker.
   */
  async getWorkerNotifications(id: MinionWorkerId): Promise<Array<[string, ...any[]]>> {
    const results = await this._pool.query<ReceiveResult>(`
      UPDATE minion_workers AS new SET inbox = '[]'
      FROM (SELECT id, inbox FROM minion_workers WHERE id = $1 FOR UPDATE) AS old
      WHERE new.id = old.id AND old.inbox != '[]'
      RETURNING old.inbox AS inbox
    `, [id]);
    if ((results.rowCount ?? 0) <= 0) return [];
    return results.rows[0].inbox ?? [];
  }


  /**
   * Get statistics for the job queue.
   */
  async stats(): Promise<MinionStats> {
    const results = await this._pool.query<MinionStats>(`
      SELECT
        (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM minion_jobs_id_seq) AS "enqueuedJobs",
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'pending' AND (expires_at IS NULL OR expires_at > NOW())) AS "pendingJobs",
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'pending' AND delay_until > NOW()) AS "delayedJobs",
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'running') AS "runningJobs",
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'succeeded') AS "succeededJobs",
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'failed') AS "failedJobs",

        (SELECT COUNT(*) FROM minion_workers) AS "workers",
        (SELECT COUNT(DISTINCT worker_id) FROM minion_jobs mj WHERE state = 'running') AS "busyWorkers",

        EXTRACT(EPOCH FROM NOW() - PG_POSTMASTER_START_TIME()) AS "uptime"
    `);

    const stats = results.rows[0];
    stats.idleWorkers = stats.workers - stats.busyWorkers;
    return stats;
  }

  /**
   * Repair worker registry and job queue if necessary.
   */
  async repair(options: RepairOptions): Promise<void> {
    // Workers without heartbeat
    await this._pool.query(`
      DELETE FROM minion_workers WHERE last_seen_at < NOW() - INTERVAL '1 millisecond' * $1
    `, [options.missingAfter]);

    // Old jobs
    await this._pool.query(`
      DELETE FROM minion_jobs
      WHERE state = 'succeeded' AND finished_at <= NOW() - INTERVAL '1 millisecond' * $1
    `, [options.removeAfter]);

    // Expired jobs
    await this._pool.query("DELETE FROM minion_jobs WHERE state = 'pending' AND expires_at <= NOW()");

    // Jobs with missing worker (can be retried)
    const jobs = await this._pool.query<JobWithMissingWorkerResult>(`
      SELECT id, retries
      FROM minion_jobs AS j
      WHERE state = 'running'
        AND queue_name != 'minion_foreground'
        AND NOT EXISTS (SELECT 1 FROM minion_workers WHERE id = j.worker_id)
    `);
    for (const job of jobs.rows) {
      await this.markJobFailed(job.id, job.retries, 'Worker went away');
    }

    // Jobs in queue without workers or not enough workers (cannot be retried and requires admin attention)
    await this._pool.query(`
      UPDATE minion_jobs SET
        state = 'failed',
        result = '"Job appears stuck in queue"'
      WHERE state = 'pending' AND delay_until + $1 * INTERVAL '1 millisecond' < NOW()
    `, [options.stuckAfter]);
  }

  /**
   * Update database schema to latest version.
   */
  async updateSchema(): Promise<void> {
    const version = (await this._pool.query<ServerVersionResult>('SHOW server_version_num')).rows[0].server_version_num;
    if (version < 90500) throw new Error('PostgreSQL 9.5 or later is required');

    const conn = await this._pool.connect();
    try {
      const migration = new Migration('minion', minionDbUpgrades, conn);
      await migration.migrate();
    } finally {
      conn.release();
    }
  }

  /**
   * Reset job queue.
   */
  async reset(options: ResetOptions): Promise<void> {
    if (options.all === true) await this._pool.query('TRUNCATE minion_jobs, minion_workers RESTART IDENTITY');
  }

  /**
   * Stop using the backend.
   */
  async end(): Promise<void> {
    if (this.autoclosePool) await this._pool.end();
  }


  protected async tryGetNextJob(workerId: MinionWorkerId, taskNames: string[], options: DequeueOptions): Promise<DequeuedJob | null> {
    const jobId = options.id;
    const minPriority = options.minPriority;
    const queueNames = options.queueNames ?? ['default'];

    const results = await this._pool.query<DequeueResult>(`
      UPDATE minion_jobs SET started_at = NOW(), state = 'running', worker_id = $1
      WHERE id = (
        SELECT id FROM minion_jobs AS j
        WHERE delay_until <= NOW()
          AND id = COALESCE($2, id)
          AND (
            parent_job_ids = '{}'
            OR NOT EXISTS (
              SELECT 1 FROM minion_jobs
              WHERE id = ANY (j.parent_job_ids)
                AND (
                  state = 'running'
                  OR (state = 'failed' AND NOT j.lax_dependency)
                  OR (state = 'pending' AND (expires_at IS NULL OR expires_at > NOW()))
                )
            )
          )
          AND priority >= COALESCE($3, priority)
          AND queue_name = ANY ($4)
          AND state = 'pending'
          AND task_name = ANY ($5)
          AND (expires_at IS NULL OR expires_at > NOW()
        )
        ORDER BY priority DESC, id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING id, args, retries, task_name AS "taskName"
    `, [workerId, jobId, minPriority, queueNames, taskNames]);
    if ((results.rowCount ?? 0) <= 0) return null;

    return results.rows[0] ?? null;
  }

  protected async updateJobAfterRun(state: 'succeeded' | 'failed', id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    const jsonResult = JSON.stringify(result);
    const results = await this._pool.query<UpdateResult>(`
      UPDATE minion_jobs SET finished_at = NOW(), result = $1, state = $2
      WHERE id = $3 AND retries = $4 AND state = 'running'
      RETURNING attempts
    `, [jsonResult, state, id, retries]);

    if ((results.rowCount ?? 0) <= 0) return false;
    return state === 'failed' ? this.autoRetryJob(id, retries, results.rows[0].attempts) : true;
  }

  protected async autoRetryJob(id: number, retries: number, attempts: number): Promise<boolean> {
    if (attempts <= 1) return true;
    const delay = this.calcBackoff(retries);
    return this.retryJob(id, retries, {attempts: attempts > 1 ? attempts - 1 : 1, delayUntil: delay});
  }

}

function removeTotal<T extends Array<{total?: number}>>(results: T): number {
  let total = 0;
  for (const result of results) {
    if (result.total !== undefined) total = result.total;
    delete result.total;
  }
  return total;
}

const minionDbUpgrades: MigrationStep[] = [
  {
    version: 1,
    sql: `
      CREATE TYPE minion_state AS ENUM ('pending', 'running', 'succeeded', 'failed');

      CREATE TABLE minion_jobs (
        id             BIGSERIAL NOT NULL PRIMARY KEY,

        queue_name     TEXT NOT NULL DEFAULT 'default',
        task_name      TEXT NOT NULL,
        args           JSONB NOT NULL CHECK(JSONB_TYPEOF(args) = 'array'),
        result         JSONB,

        state          minion_state NOT NULL DEFAULT 'pending'::MINION_STATE,
        priority       INT NOT NULL,
        attempts       INT NOT NULL DEFAULT 1,
        retries        INT NOT NULL DEFAULT 0,

        parent_job_ids BIGINT[] NOT NULL DEFAULT '{}',
        lax_dependency BOOL NOT NULL DEFAULT FALSE,

        worker_id      BIGINT,
        notes          JSONB CHECK(JSONB_TYPEOF(notes) = 'object') NOT NULL DEFAULT '{}',

        delay_until    TIMESTAMP WITH TIME ZONE NOT NULL,
        started_at     TIMESTAMP WITH TIME ZONE,
        retried_at     TIMESTAMP WITH TIME ZONE,
        finished_at   TIMESTAMP WITH TIME ZONE,

        created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        expires_at     TIMESTAMP WITH TIME ZONE
      );
      CREATE INDEX ON minion_jobs (state, priority DESC, id);
      CREATE INDEX ON minion_jobs USING GIN (parent_job_ids);
      CREATE INDEX ON minion_jobs USING GIN (notes);
      CREATE INDEX ON minion_jobs (expires_at);
      CREATE INDEX ON minion_jobs (finished_at, state);
      CREATE FUNCTION minion_jobs_notify_workers() RETURNS trigger AS $$
        BEGIN
          IF new.delay_until <= NOW() THEN
            NOTIFY "minion.job";
          END IF;
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
      CREATE TRIGGER minion_jobs_notify_workers_trigger
        AFTER INSERT OR UPDATE OF retries ON minion_jobs
        FOR EACH ROW EXECUTE PROCEDURE minion_jobs_notify_workers();

      CREATE UNLOGGED TABLE minion_workers (
        id           BIGSERIAL NOT NULL PRIMARY KEY,

        host         TEXT NOT NULL,
        pid          INT NOT NULL,

        status       JSONB CHECK(JSONB_TYPEOF(status) = 'object') NOT NULL DEFAULT '{}',

        inbox        JSONB CHECK(JSONB_TYPEOF(inbox) = 'array') NOT NULL DEFAULT '[]',

        started_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
      );
      `,
  }
];

interface DequeueResult {
  id: MinionJobId;
  args: MinionArgs;
  retries: number;
  taskName: string;
}

interface EnqueueResult {
  id: MinionJobId;
}

interface JobWithMissingWorkerResult {
  id: MinionJobId;
  retries: number;
}

interface ListJobsResult extends JobInfo {
  total: number;
}

interface ListWorkersResult extends WorkerInfo {
  total: number;
}

interface ReceiveResult {
  inbox: Array<[string, ...any[]]>;
}

interface RegisterWorkerResult {
  id: MinionWorkerId;
}

interface ServerVersionResult {
  server_version_num: number;
}

interface UpdateResult {
  attempts: number;
}
