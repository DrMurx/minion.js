import os from 'node:os';
import pg from 'pg';
import { Migration, MigrationStep } from './migration.js';
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
} from './types.js';
import { defaultBackoffStrategy } from './minion.js';

interface DequeueResult {
  id: MinionJobId;
  args: MinionArgs;
  retries: number;
  task: string;
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

/**
 * Minion PostgreSQL backend class.
 */
export class PgBackend implements MinionBackend {
  /**
   * Backend name.
   */
  public readonly name = 'Pg';

  private hostname = os.hostname();

  constructor(private pool: pg.Pool, private calcBackoff: MinionBackoffStrategy = defaultBackoffStrategy) {}

  /**
   * Parse PostgreSQL connection URI.
   */
  static parseConfig(config: string): pg.PoolConfig {
    const url = new URL(config);
    if (url.protocol.match('/^postgres(ql)?:$/')) throw new TypeError(`Invalid URL: ${config}`);

    const poolConfig: pg.PoolConfig = {};
    if (url.hostname !== '') poolConfig.host = decodeURIComponent(url.hostname);
    if (url.port !== '') poolConfig.port = parseInt(url.port);
    if (url.username !== '') poolConfig.user = url.username;
    if (url.password !== '') poolConfig.password = url.password;
    if (url.pathname.startsWith('/')) poolConfig.database = decodeURIComponent(url.pathname.slice(1));
    const currentSchema = url.searchParams.get('currentSchema');
    if (currentSchema !== null) poolConfig.options = `-c search_path=${currentSchema}`;

    return poolConfig;
  }

  static connect(config: string): pg.Pool {
    pg.types.setTypeParser(20, parseInt);
    return new pg.Pool({allowExitOnIdle: true, ...PgBackend.parseConfig(config)});
  }

  /**
   * Enqueue a new job with `inactive` state.
   */
  async addJob(task: string, args: MinionArgs = [], options: EnqueueOptions = {}): Promise<MinionJobId> {
    const results = await this.pool.query<EnqueueResult>(
      `
        INSERT INTO minion_jobs (args, attempts, delayed, expires, lax, notes, parents, priority, queue, task)
        VALUES ($1, $2, (NOW() + (INTERVAL '1 millisecond' * $3)),
          CASE WHEN $4::BIGINT IS NOT NULL THEN NOW() + (INTERVAL '1 millisecond' * $4::BIGINT) END,
          $5, $6, $7, $8, $9, $10
        )
        RETURNING id
      `,
      [JSON.stringify(args),
      options.attempts ?? 1,
      options.delay ?? 0,
      options.expire,
      options.lax ?? false,
      options.notes ?? {},
      options.parents ?? [],
      options.priority ?? 0,
      options.queue ?? 'default',
      task]
    );

    return results.rows[0].id;
  }

  /**
   * Wait a given amount of time in milliseconds for a job, dequeue it and transition from `inactive` to `active`
   * state, or return `null` if queues were empty.
   */
  async getNextJob(id: MinionWorkerId, tasks: string[], wait: number, options: DequeueOptions): Promise<DequeuedJob | null> {
    const job = await this.tryGetNextJob(id, tasks, options);
    if (job !== null) return job;

    const conn = await this.pool.connect();
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

    return await this.tryGetNextJob(id, tasks, options);
  }

  /**
   * Transition from `active` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `inactive` with a delay.
   */
  async markJobFailed(id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    return await this.updateJobAfterRun('failed', id, retries, result);
  }

  /**
   * Transition from C<active> to `finished` state with or without a result.
   */
  async markJobFinished(id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    return await this.updateJobAfterRun('finished', id, retries, result);
  }

  /**
   * Transition job back to `inactive` state, already `inactive` jobs may also be retried to change options.
   */
  async retryJob(id: MinionJobId, retries: number, options: RetryOptions = {}): Promise<boolean> {
    const results = await this.pool.query(
      `
        UPDATE minion_jobs SET attempts = COALESCE($1, attempts), delayed = (NOW() + (INTERVAL '1 millisecond' * $2)),
          expires =
            CASE WHEN $3::BIGINT IS NOT NULL THEN
              NOW() + (INTERVAL '1 millisecond' * $3::BIGINT)
            ELSE
              expires
            END,
          lax = COALESCE($4, lax), parents = COALESCE($5, parents), priority = COALESCE($6, priority),
          queue = COALESCE($7, queue), retried = NOW(), retries = retries + 1, state = 'inactive'
        WHERE id = $8 AND retries = $9
      `,
      [options.attempts, options.delay ?? 0, options.expire, options.lax, options.parents, options.priority, options.queue, id, retries]
    );

    return (results.rowCount ?? 0) > 0 ? true : false;
  }

  /**
   * Remove `failed`, `finished` or `inactive` job from queue.
   */
  async removeJob(id: MinionJobId): Promise<boolean> {
    const results = await this.pool.query(
      "DELETE FROM minion_jobs WHERE id = $1 AND state IN ('inactive', 'failed', 'finished')", [id]);
    return (results.rowCount ?? 0) > 0;
  }

  /**
   * Returns the information about jobs in batches.
   */
  async getJobInfos(offset: number, limit: number, options: ListJobsOptions = {}): Promise<JobList> {
    const results = await this.pool.query<ListJobsResult>(
      `
        SELECT id, args, attempts, ARRAY(SELECT id FROM minion_jobs WHERE parents @> ARRAY[j.id]) AS children, created,
          delayed, expires, finished, lax, notes, parents, priority, queue, result, retried, retries, started, state,
          task, now() AS time, COUNT(*) OVER() AS total, worker
        FROM minion_jobs AS j
        WHERE (id < $1 OR $1 IS NULL) AND (id = ANY ($2) OR $2 IS NULL) AND (notes ? ANY ($3) OR $3 IS NULL)
          AND (queue = ANY ($4) OR $4 IS null) AND (state = ANY ($5) OR $5 IS NULL) AND (task = ANY ($6) OR $6 IS NULL)
          AND (state != 'inactive' OR expires IS null OR expires > NOW())
        ORDER BY id DESC
        LIMIT $7 OFFSET $8
      `,
      [options.before, options.ids, options.notes, options.queues, options.states, options.tasks, limit, offset]
    );
    const total = removeTotal(results.rows);
    return {total, jobs: results.rows};
  }

  /**
   * Get history information for job queue.
   */
  async getJobHistory(): Promise<MinionHistory> {
    const results = await this.pool.query<DailyHistory>(`
      SELECT EXTRACT(EPOCH FROM ts) AS epoch, COALESCE(failed_jobs, 0) AS failed_jobs,
        COALESCE(finished_jobs, 0) AS finished_jobs
      FROM (
        SELECT EXTRACT (DAY FROM finished) AS day, EXTRACT(HOUR FROM finished) AS hour,
          COUNT(*) FILTER (WHERE state = 'failed') AS failed_jobs,
          COUNT(*) FILTER (WHERE state = 'finished') AS finished_jobs
        FROM minion_jobs
        WHERE finished > NOW() - INTERVAL '23 hours'
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
    const results = await this.pool.query(
      'UPDATE minion_jobs SET notes = JSONB_STRIP_NULLS(notes || $1) WHERE id = $2', [notes, id]);
    return (results.rowCount ?? 0) > 0;
  }


  /**
   * Register worker or send heartbeat to show that this worker is still alive.
   */
  async registerWorker(id?: MinionWorkerId, options: RegisterWorkerOptions = {}): Promise<MinionWorkerId> {
    const status = options.status ?? {};
    const results = await this.pool.query<RegisterWorkerResult>(`
      INSERT INTO minion_workers (id, host, pid, status)
        VALUES (COALESCE($1, NEXTVAL('minion_workers_id_seq')), $2, $3, $4)
        ON CONFLICT(id) DO UPDATE SET notified = now(), status = $4
        RETURNING id
    `, [id, this.hostname, process.pid, status]);
    return results.rows[0].id;
  }

  /**
   * Unregister worker.
   */
  async unregisterWorker(id: MinionWorkerId): Promise<void> {
    await this.pool.query('DELETE FROM minion_workers WHERE id = $1', [id]);
  }

  /**
   * Returns information about workers in batches.
   */
  async getWorkers(offset: number, limit: number, options: ListWorkersOptions = {}): Promise<WorkerList> {
    const results = await this.pool.query<ListWorkersResult>(
      `
        SELECT id, notified, ARRAY(
            SELECT id FROM minion_jobs WHERE state = 'active' AND worker = minion_workers.id
        ) AS jobs, host, pid, status, started, COUNT(*) OVER() AS total
        FROM minion_workers
        WHERE (id < $1 OR $1 IS NULL) AND (id = ANY ($2) OR $2 IS NULL)
        ORDER BY id DESC LIMIT $3 OFFSET $4
      `,
      [options.before, options.ids, limit, offset]
    );
    const total = removeTotal(results.rows);
    return {total, workers: results.rows};
  }

  /**
   * Broadcast remote control command to one or more workers.
   */
  async notifyWorkers(command: string, args: any[] = [], ids: MinionWorkerId[] = []): Promise<boolean> {
    const results = await this.pool.query(
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
    const results = await this.pool.query<ReceiveResult>(`
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
    const results = await this.pool.query<MinionStats>(`
      SELECT
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'inactive' AND (expires IS NULL OR expires > NOW()))
          AS inactive_jobs,
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'active') AS active_jobs,
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'failed') AS failed_jobs,
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'finished') AS finished_jobs,
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'inactive' AND delayed > NOW()) AS delayed_jobs,
        (SELECT COUNT(DISTINCT worker) FROM minion_jobs mj WHERE state = 'active') AS active_workers,
        (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM minion_jobs_id_seq) AS enqueued_jobs,
        (SELECT COUNT(*) FROM minion_workers) AS workers,
        EXTRACT(EPOCH FROM NOW() - PG_POSTMASTER_START_TIME()) AS uptime
    `);

    const stats = results.rows[0];
    stats.inactive_workers = stats.workers - stats.active_workers;
    return stats;
  }

  /**
   * Repair worker registry and job queue if necessary.
   */
  async repair(options: RepairOptions): Promise<void> {
    // Workers without heartbeat
    await this.pool.query(`
      DELETE FROM minion_workers WHERE notified < NOW() - INTERVAL '1 millisecond' * $1
    `, [options.missingAfter]);

    // Old jobs
    await this.pool.query(`
      DELETE FROM minion_jobs
      WHERE state = 'finished' AND finished <= NOW() - INTERVAL '1 millisecond' * $1
    `, [options.removeAfter]);

    // Expired jobs
    await this.pool.query("DELETE FROM minion_jobs WHERE state = 'inactive' AND expires <= NOW()");

    // Jobs with missing worker (can be retried)
    const jobs = await this.pool.query<JobWithMissingWorkerResult>(`
      SELECT id, retries FROM minion_jobs AS j
      WHERE state = 'active' AND queue != 'minion_foreground'
        AND NOT EXISTS (SELECT 1 FROM minion_workers WHERE id = j.worker)
    `);
    for (const job of jobs.rows) {
      await this.markJobFailed(job.id, job.retries, 'Worker went away');
    }

    // Jobs in queue without workers or not enough workers (cannot be retried and requires admin attention)
    await this.pool.query(`
      UPDATE minion_jobs SET state = 'failed', result = '"Job appears stuck in queue"'
          WHERE state = 'inactive' AND delayed + $1 * INTERVAL '1 millisecond' < NOW()
    `, [options.stuckAfter]);
  }

  /**
   * Update database schema to latest version.
   */
  async updateSchema(): Promise<void> {
    const version = (await this.pool.query<ServerVersionResult>('SHOW server_version_num')).rows[0].server_version_num;
    if (version < 90500) throw new Error('PostgreSQL 9.5 or later is required');

    const conn = await this.pool.connect();
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
    if (options.all === true) await this.pool.query('TRUNCATE minion_jobs, minion_workers RESTART IDENTITY');
  }

  /**
   * Stop using the queue.
   */
  async end(): Promise<void> {
  }


  protected async tryGetNextJob(id: MinionWorkerId, tasks: string[], options: DequeueOptions): Promise<DequeuedJob | null> {
    const jobId = options.id;
    const minPriority = options.minPriority;
    const queues = options.queues ?? ['default'];

    const results = await this.pool.query<DequeueResult>(`
      UPDATE minion_jobs SET started = NOW(), state = 'active', worker = ${id}
      WHERE id = (
        SELECT id FROM minion_jobs AS j
        WHERE delayed <= NOW() AND id = COALESCE($1, id) AND (parents = '{}' OR NOT EXISTS (
          SELECT 1 FROM minion_jobs WHERE id = ANY (j.parents) AND (
            state = 'active' OR (state = 'failed' AND NOT j.lax)
            OR (state = 'inactive' AND (expires IS NULL OR expires > NOW())))
        )) AND priority >= COALESCE($2, priority) AND queue = ANY ($3) AND state = 'inactive'
          AND task = ANY ($4) AND (EXPIRES IS NULL OR expires > NOW())
        ORDER BY priority DESC, id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING id, args, retries, task
    `, [jobId, minPriority, queues, tasks]);
    if ((results.rowCount ?? 0) <= 0) return null;

    return results.rows[0] ?? null;
  }

  protected async updateJobAfterRun(state: 'finished' | 'failed', id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    const jsonResult = JSON.stringify(result);
    const results = await this.pool.query<UpdateResult>(`
      UPDATE minion_jobs SET finished = NOW(), result = $1, state = $2
      WHERE id = $3 AND retries = $4 AND state = 'active'
      RETURNING attempts
    `, [jsonResult, state, id, retries]);

    if ((results.rowCount ?? 0) <= 0) return false;
    return state === 'failed' ? this.autoRetryJob(id, retries, results.rows[0].attempts) : true;
  }

  protected async autoRetryJob(id: number, retries: number, attempts: number): Promise<boolean> {
    if (attempts <= 1) return true;
    const delay = this.calcBackoff(retries);
    return this.retryJob(id, retries, {attempts: attempts > 1 ? attempts - 1 : 1, delay});
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
    version: 10,
    sql: `
      CREATE TYPE minion_state AS ENUM ('inactive', 'active', 'failed', 'finished');
      CREATE TABLE IF NOT EXISTS minion_jobs (
        id       BIGSERIAL NOT NULL PRIMARY KEY,
        args     JSONB NOT NULL CHECK(JSONB_TYPEOF(args) = 'array'),
        attempts INT NOT NULL DEFAULT 1,
        created  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        delayed  TIMESTAMP WITH TIME ZONE NOT NULL,
        finished TIMESTAMP WITH TIME ZONE,
        notes    JSONB CHECK(JSONB_TYPEOF(notes) = 'object') NOT NULL DEFAULT '{}',
        parents  BIGINT[] NOT NULL DEFAULT '{}',
        priority INT NOT NULL,
        queue    TEXT NOT NULL DEFAULT 'default',
        result   JSONB,
        retried  TIMESTAMP WITH TIME ZONE,
        retries  INT NOT NULL DEFAULT 0,
        started  TIMESTAMP WITH TIME ZONE,
        state    minion_state NOT NULL DEFAULT 'inactive'::MINION_STATE,
        task     TEXT NOT NULL,
        worker   BIGINT
      );
      CREATE INDEX ON minion_jobs (state, priority DESC, id);
      CREATE INDEX ON minion_jobs USING GIN (parents);
      CREATE TABLE IF NOT EXISTS minion_workers (
        id       BIGSERIAL NOT NULL PRIMARY KEY,
        host     TEXT NOT NULL,
        inbox    JSONB CHECK(JSONB_TYPEOF(inbox) = 'array') NOT NULL DEFAULT '[]',
        notified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        pid      INT NOT NULL,
        started  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        status   JSONB CHECK(JSONB_TYPEOF(status) = 'object') NOT NULL DEFAULT '{}'
      );

      CREATE OR REPLACE FUNCTION minion_jobs_notify_workers() RETURNS trigger AS $$
        BEGIN
          IF new.delayed <= NOW() THEN
            NOTIFY "minion.job";
          END IF;
          RETURN NULL;
        END;
      $$ LANGUAGE plpgsql;
      CREATE TRIGGER minion_jobs_notify_workers_trigger
        AFTER INSERT OR UPDATE OF retries ON minion_jobs
        FOR EACH ROW EXECUTE PROCEDURE minion_jobs_notify_workers();
      `,
  },
  {
    version: 19,
    sql: `CREATE INDEX ON minion_jobs USING GIN (notes);`,
  },
  {
    version: 20,
    sql: `ALTER TABLE minion_workers SET UNLOGGED;`,
  },
  {
    version: 22,
    sql: `
      ALTER TABLE minion_jobs DROP COLUMN IF EXISTS SEQUENCE;
      ALTER TABLE minion_jobs DROP COLUMN IF EXISTS NEXT;
      ALTER TABLE minion_jobs ADD COLUMN EXPIRES TIMESTAMP WITH TIME ZONE;
      CREATE INDEX ON minion_jobs (expires);
      `,
  },
  {
    version: 23,
    sql: `ALTER TABLE minion_jobs ADD COLUMN lax BOOL NOT NULL DEFAULT FALSE;`,
  },
  {
    version: 24,
    sql: `CREATE INDEX ON minion_jobs (finished, state);`,
  }
];
