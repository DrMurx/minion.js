import type {Minion} from './minion.js';
import type {
  DailyHistory,
  DequeueOptions,
  DequeuedJob,
  EnqueueOptions,
  JobInfo,
  JobList,
  ListJobsOptions,
  ListLocksOptions,
  ListWorkersOptions,
  LockInfo,
  LockOptions,
  LockList,
  MinionArgs,
  MinionHistory,
  MinionJobId,
  MinionStats,
  MinionWorkerId,
  RegisterWorkerOptions,
  ResetOptions,
  RetryOptions,
  WorkerInfo,
  WorkerList
} from './types.js';
import os from 'node:os';
import Pg, { PgConfig } from './pg/index.js';

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

interface ListLockResult extends LockInfo {
  total: number;
}

interface ListWorkersResult extends WorkerInfo {
  total: number;
}

interface LockResult {
  minion_lock: boolean;
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
export class PgBackend {
  /**
   * Backend name.
   */
  name = 'Pg';
  /**
   * Minion instance this backend belongs to.
   */
  minion: Minion;
  /**
   * `pg` object used to store all data.
   */
  pg: Pg;

  _hostname = os.hostname();

  constructor(minion: Minion, config: PgConfig) {
    this.minion = minion;
    this.pg = new Pg(config);
  }

  /**
   * Broadcast remote control command to one or more workers.
   */
  async broadcast(command: string, args: any[] = [], ids: MinionWorkerId[] = []): Promise<boolean> {
    const results = await this.pg.query(
      `
        UPDATE minion_workers SET inbox = inbox || $1::JSONB
        WHERE (id = ANY ($2) OR $2 = '{}')
      `,
      JSON.stringify([[command, ...args]]),
      ids
    );
    return (results.count ?? 0) > 0;
  }

  /**
   * Wait a given amount of time in milliseconds for a job, dequeue it and transition from `inactive` to `active`
   * state, or return `null` if queues were empty.
   */
  async dequeue(id: MinionWorkerId, wait: number, options: DequeueOptions): Promise<DequeuedJob | null> {
    const job = await this._try(id, options);
    if (job !== null) return job;

    const db = await this.pg.db();
    try {
      await db.listen('minion.job');
      let timer;
      await Promise.race([
        new Promise(resolve => db.on('notification', resolve)),
        new Promise(resolve => (timer = setTimeout(resolve, wait)))
      ]);
      clearTimeout(timer);
    } finally {
      await db.release();
    }

    return await this._try(id, options);
  }

  /**
   * Stop using the queue.
   */
  async end(): Promise<void> {
    await this.pg.end();
  }

  /**
   * Enqueue a new job with `inactive` state.
   */
  async enqueue(task: string, args: MinionArgs = [], options: EnqueueOptions = {}): Promise<MinionJobId> {
    const results = await this.pg.query<EnqueueResult>(
      `
        INSERT INTO minion_jobs (args, attempts, delayed, expires, lax, notes, parents, priority, queue, task)
        VALUES ($1, $2, (NOW() + (INTERVAL '1 millisecond' * $3)),
          CASE WHEN $4::BIGINT IS NOT NULL THEN NOW() + (INTERVAL '1 millisecond' * $4::BIGINT) END,
          $5, $6, $7, $8, $9, $10
        )
        RETURNING id
      `,
      JSON.stringify(args),
      options.attempts ?? 1,
      options.delay ?? 0,
      options.expire,
      options.lax ?? false,
      options.notes ?? {},
      options.parents ?? [],
      options.priority ?? 0,
      options.queue ?? 'default',
      task
    );

    return results.first.id;
  }

  /**
   * Transition from `active` to `failed` state with or without a result, and if there are attempts remaining,
   * transition back to `inactive` with a delay.
   */
  async failJob(id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    return await this._update('failed', id, retries, result);
  }

  /**
   * Transition from C<active> to `finished` state with or without a result.
   */
  async finishJob(id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    return await this._update('finished', id, retries, result);
  }

  /**
   * Get history information for job queue.
   */
  async history(): Promise<MinionHistory> {
    const results = await this.pg.query<DailyHistory>(`
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
    return {daily: results};
  }

  /**
   * Returns the information about jobs in batches.
   */
  async listJobs(offset: number, limit: number, options: ListJobsOptions = {}): Promise<JobList> {
    const results = await this.pg.query<ListJobsResult>(
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
      options.before,
      options.ids,
      options.notes,
      options.queues,
      options.states,
      options.tasks,
      limit,
      offset
    );

    return {total: removeTotal(results), jobs: results};
  }

  /**
   * Returns information about locks in batches.
   */
  async listLocks(offset: number, limit: number, options: ListLocksOptions = {}): Promise<LockList> {
    const results = await this.pg.query<ListLockResult>(
      `
        SELECT name, expires, COUNT(*) OVER() AS total FROM minion_locks
        WHERE expires > NOW() AND (name = ANY ($1) OR $1 IS NULL)
        ORDER BY id DESC LIMIT $2 OFFSET $3
      `,
      options.names,
      limit,
      offset
    );

    return {total: removeTotal(results), locks: results};
  }

  /**
   * Returns information about workers in batches.
   */
  async listWorkers(offset: number, limit: number, options: ListWorkersOptions = {}): Promise<WorkerList> {
    const results = await this.pg.query<ListWorkersResult>(
      `
        SELECT id, notified, ARRAY(
            SELECT id FROM minion_jobs WHERE state = 'active' AND worker = minion_workers.id
        ) AS jobs, host, pid, status, started, COUNT(*) OVER() AS total
        FROM minion_workers
        WHERE (id < $1 OR $1 IS NULL) AND (id = ANY ($2) OR $2 IS NULL)
        ORDER BY id DESC LIMIT $3 OFFSET $4
      `,
      options.before,
      options.ids,
      limit,
      offset
    );

    return {total: removeTotal(results), workers: results};
  }

  /**
   * Try to acquire a named lock that will expire automatically after the given amount of time in milliseconds. An
   * expiration time of `0` can be used to check if a named lock already exists without creating one.
   */
  async lock(name: string, duration: number, options: LockOptions = {}): Promise<boolean> {
    const limit = options.limit ?? 1;
    const results = await this.pg.query<LockResult>('SELECT * FROM minion_lock($1, $2, $3)', name, duration / 1000, limit);
    return results.first.minion_lock;
  }

  /**
   * Change one or more metadata fields for a job. Setting a value to `null` will remove the field.
   */
  async note(id: MinionJobId, merge: Record<string, any>): Promise<boolean> {
    const results = await this.pg
      .query('UPDATE minion_jobs SET notes = JSONB_STRIP_NULLS(notes || $1) WHERE id = $2', merge, id);
    return (results.count ?? 0) > 0;
  }

  /**
   * Receive remote control commands for worker.
   */
  async receive(id: MinionWorkerId): Promise<Array<[string, ...any[]]>> {
    const results = await this.pg.query<ReceiveResult>(`
      UPDATE minion_workers AS new SET inbox = '[]'
      FROM (SELECT id, inbox FROM minion_workers WHERE id = $1 FOR UPDATE) AS old
      WHERE new.id = old.id AND old.inbox != '[]'
      RETURNING old.inbox AS inbox
    `, id);
    return results.first?.inbox ?? [];
  }

  /**
   * Register worker or send heartbeat to show that this worker is still alive.
   */
  async registerWorker(id?: MinionWorkerId, options: RegisterWorkerOptions = {}): Promise<MinionWorkerId> {
    const status = options.status ?? {};
    const results = await this.pg.query<RegisterWorkerResult>(`
      INSERT INTO minion_workers (id, host, pid, status)
        VALUES (COALESCE($1, NEXTVAL('minion_workers_id_seq')), $2, $3, $4)
        ON CONFLICT(id) DO UPDATE SET notified = now(), status = $4
        RETURNING id
    `, id, this._hostname, process.pid, status);
    return results.first.id;
  }

  /**
   * Remove `failed`, `finished` or `inactive` job from queue.
   */
  async removeJob(id: MinionJobId): Promise<boolean> {
    const results = await this.pg
      .query("DELETE FROM minion_jobs WHERE id = $1 AND state IN ('inactive', 'failed', 'finished')", id);
    return (results.count ?? 0) > 0 ? true : false;
  }

  /**
   * Repair worker registry and job queue if necessary.
   */
  async repair(): Promise<void> {
    const pg = this.pg;
    const minion = this.minion;

    // Workers without heartbeat
    await pg.query(`
      DELETE FROM minion_workers WHERE notified < NOW() - INTERVAL '1 millisecond' * $1
    `, minion.missingAfter);

    // Old jobs
    await pg.query(`
      DELETE FROM minion_jobs
      WHERE state = 'finished' AND finished <= NOW() - INTERVAL '1 millisecond' * $1
    `, minion.removeAfter);

    // Expired jobs
    await pg.query("DELETE FROM minion_jobs WHERE state = 'inactive' AND expires <= NOW()");

    // Jobs with missing worker (can be retried)
    const jobs = await pg.query<JobWithMissingWorkerResult>(`
      SELECT id, retries FROM minion_jobs AS j
      WHERE state = 'active' AND queue != 'minion_foreground'
        AND NOT EXISTS (SELECT 1 FROM minion_workers WHERE id = j.worker)
    `);
    for (const job of jobs) {
      await this.failJob(job.id, job.retries, 'Worker went away');
    }

    // Jobs in queue without workers or not enough workers (cannot be retried and requires admin attention)
    await pg.query(`
      UPDATE minion_jobs SET state = 'failed', result = '"Job appears stuck in queue"'
          WHERE state = 'inactive' AND delayed + $1 * INTERVAL '1 millisecond' < NOW()
    `, minion.stuckAfter);
  }

  /**
   * Reset job queue.
   */
  async reset(options: ResetOptions): Promise<void> {
    if (options.all === true) await this.pg.query('TRUNCATE minion_jobs, minion_locks, minion_workers RESTART IDENTITY');
    if (options.locks === true) await this.pg.query('TRUNCATE minion_locks');
  }

  /**
   * Transition job back to `inactive` state, already `inactive` jobs may also be retried to change options.
   */
  async retryJob(id: MinionJobId, retries: number, options: RetryOptions = {}): Promise<boolean> {
    const results = await this.pg.query(
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
      options.attempts,
      options.delay ?? 0,
      options.expire,
      options.lax,
      options.parents,
      options.priority,
      options.queue,
      id,
      retries
    );

    return (results.count ?? 0) > 0 ? true : false;
  }

  /**
   * Get statistics for the job queue.
   */
  async stats(): Promise<MinionStats> {
    const results = await this.pg.query<MinionStats>(`
      SELECT
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'inactive' AND (expires IS NULL OR expires > NOW()))
          AS inactive_jobs,
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'active') AS active_jobs,
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'failed') AS failed_jobs,
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'finished') AS finished_jobs,
        (SELECT COUNT(*) FROM minion_jobs WHERE state = 'inactive' AND delayed > NOW()) AS delayed_jobs,
        (SELECT COUNT(*) FROM minion_locks WHERE expires > NOW()) AS active_locks,
        (SELECT COUNT(DISTINCT worker) FROM minion_jobs mj WHERE state = 'active') AS active_workers,
        (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM minion_jobs_id_seq) AS enqueued_jobs,
        (SELECT COUNT(*) FROM minion_workers) AS workers,
        EXTRACT(EPOCH FROM NOW() - PG_POSTMASTER_START_TIME()) AS uptime
    `);

    const stats = results.first;
    stats.inactive_workers = stats.workers - stats.active_workers;
    return stats;
  }

  /**
   * Release a named lock.
   */
  async unlock(name: string): Promise<boolean> {
    const results = await this.pg.query(`
      DELETE FROM minion_locks WHERE id = (
        SELECT id FROM minion_locks WHERE expires > NOW() AND name = $1 ORDER BY expires LIMIT 1 FOR UPDATE
      )
    `, name);
    return (results.count ?? 0) > 0 ? true : false;
  }

  /**
   * Unregister worker.
   */
  async unregisterWorker(id: MinionWorkerId): Promise<void> {
    await this.pg.query('DELETE FROM minion_workers WHERE id = $1', id);
  }

  /**
   * Update database schema to latest version.
   */
  async update(): Promise<void> {
    const pg = this.pg;

    const version = (await pg.query<ServerVersionResult>('SHOW server_version_num')).first.server_version_num;
    if (version < 90500) throw new Error('PostgreSQL 9.5 or later is required');

    const migrations = pg.migrations;
    await migrations.fromFile('migrations/minion.sql', {name: 'minion'});
    await migrations.migrate();
  }

  async _autoRetryJob(id: number, retries: number, attempts: number): Promise<boolean> {
    if (attempts <= 1) return true;
    const delay = this.minion.backoff(retries);
    return this.retryJob(id, retries, {attempts: attempts > 1 ? attempts - 1 : 1, delay});
  }

  async _try(id: MinionWorkerId, options: DequeueOptions): Promise<DequeuedJob | null> {
    const jobId = options.id;
    const minPriority = options.minPriority;
    const queues = options.queues ?? ['default'];
    const tasks = Object.keys(this.minion.tasks);

    const results = await this.pg.query<DequeueResult>(`
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
    `, jobId, minPriority, queues, tasks);

    return results.first ?? null;
  }

  async _update(state: 'finished' | 'failed', id: MinionJobId, retries: number, result?: any): Promise<boolean> {
    const jsonResult = JSON.stringify(result);
    const results = await this.pg.query<UpdateResult>(`
      UPDATE minion_jobs SET finished = NOW(), result = $1, state = $2
      WHERE id = $3 AND retries = $4 AND state = 'active'
      RETURNING attempts
    `, jsonResult, state, id, retries);

    if (results.length <= 0) return false;
    return state === 'failed' ? this._autoRetryJob(id, retries, results.first.attempts) : true;
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
