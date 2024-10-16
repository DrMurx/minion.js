import EventEmitter from 'events';
import os from 'os';
import pg from 'pg';
import {
  type Backend,
  type JobEnqueueOptions,
  type JobDequeueOptions,
  type JobInfoList,
  type JobPruneResult,
  type WorkerInboxOptions,
  type WorkerInfoList,
  type WorkerPruneResult,
  type WorkerRegistrationOptions,
} from '../../types/backend.js';
import {
  type DailyJobHistory,
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  type JobRetryOptions,
  JobState,
  type ListJobsOptions,
  type QueueJobStatistics,
} from '../../types/job.js';
import { type QueueStats } from '../../types/queue.js';
import {
  type ListWorkersOptions,
  type WorkerCommandArg,
  type WorkerCommandDescriptor,
  type WorkerId,
  type WorkerInfo,
  WorkerState,
} from '../../types/worker.js';
import { createPool } from './factory.js';
import { Migration, type MigrationStep } from './migration.js';

export const JOB_TABLE = 'queue_jobs';
export const WORKER_TABLE = 'queue_workers';
const JOB_NOTIFICATION_CHANNEL = 'queue_job';
const JOB_NOTIFICATION_FUNCTION = 'queue_jobs_notify_workers';
const JOB_NOTIFICATION_TRIGGER = 'queue_jobs_notify_workers_trigger';

/**
 * PostgreSQL backend class for the Queue.
 */
export class PgBackend extends EventEmitter implements Backend {
  /**
   * Backend name.
   */
  public readonly name = 'Pg';

  private hostname = os.hostname();

  private _pool: pg.Pool;
  private _schema: string | undefined;
  private autoclosePool = false;

  constructor(config: string | pg.Pool) {
    super();
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

  protected async getSchema(): Promise<string> {
    if (this._schema) return this._schema;
    const results = await this._pool.query<{ current_schema: string }>(`SELECT current_schema`);
    return (this._schema = results.rows[0].current_schema);
  }

  async addJob(taskName: string, args: JobArgs, options: JobEnqueueOptions): Promise<JobId> {
    const delayFor = options.delayFor;
    const results = await this._pool.query<EnqueueResult>(
      `INSERT INTO ${JOB_TABLE} (
        queue_name,
        task_name,
        args,

        state,
        priority,
        max_attempts,
        attempt,

        parent_job_ids,
        lax_dependency,

        metadata,

        delay_until,
        expires_at
      )
      VALUES (
        $1, $2, $3,
        $4, $5, $6, $7,
        $8, $9,
        $10,
        NOW() + $11 * INTERVAL '1 millisecond',
        CASE WHEN $12::BIGINT IS NOT NULL THEN NOW() + $12::BIGINT * INTERVAL '1 millisecond' END
      )
      RETURNING id`,
      [
        options.queueName,
        taskName,
        JSON.stringify(args),
        delayFor <= 0 ? JobState.Pending : JobState.Scheduled,
        options.priority,
        options.maxAttempts,
        1,
        options.parentJobIds,
        options.laxDependency,
        options.metadata,
        delayFor,
        options.expireIn,
      ],
    );

    return results.rows[0].id;
  }

  async retryJob(id: JobId, attempt: number, options: JobRetryOptions): Promise<boolean> {
    const delayFor = options.delayFor ?? 0;
    const results = await this._pool.query(
      `UPDATE ${JOB_TABLE} SET
        queue_name = COALESCE($1, queue_name),
        state = $2,
        priority = COALESCE($3, priority),
        progress = 0.0,
        max_attempts = COALESCE($4, max_attempts + 1),
        attempt = attempt + 1,
        parent_job_ids = COALESCE($5, parent_job_ids),
        lax_dependency = COALESCE($6, lax_dependency),
        delay_until = NOW() + $7 * INTERVAL '1 millisecond',
        retried_at = NOW(),
        expires_at = CASE WHEN $8::BIGINT IS NULL THEN expires_at ELSE NOW() + $8::BIGINT * INTERVAL '1 millisecond' END
      WHERE id = $9
        AND attempt = $10
      RETURNING id, task_name AS "taskName", args, max_attempts AS "maxAttempts", attempt`,
      [
        options.queueName,
        delayFor <= 0 ? JobState.Pending : JobState.Scheduled,
        options.priority,
        options.maxAttempts,
        options.parentJobIds,
        options.laxDependency,
        delayFor,
        options.expireIn,
        id,
        attempt,
      ],
    );

    return (results.rowCount ?? 0) > 0 ? true : false;
  }

  async cancelJob(id: JobId): Promise<boolean> {
    const results = await this._pool.query(
      `UPDATE ${JOB_TABLE} SET
        state = '${JobState.Canceled}'
      WHERE id = $1
        AND state IN (
          '${JobState.Pending}',
          '${JobState.Scheduled}'
        )`,
      [id],
    );
    return (results.rowCount ?? 0) > 0;
  }

  async amendJobMetadata(id: JobId, records: Record<string, any>): Promise<boolean> {
    const results = await this._pool.query(
      `UPDATE ${JOB_TABLE}
      SET metadata = JSONB_STRIP_NULLS(metadata || $1)
      WHERE id = $2`,
      [records, id],
    );
    return (results.rowCount ?? 0) > 0;
  }

  async updateJobProgress(id: JobId, attempt: number, progress: number): Promise<boolean> {
    const results = await this._pool.query(
      `UPDATE ${JOB_TABLE}
      SET progress = $1
      WHERE id = $2
        AND attempt = $3`,
      [progress, id, attempt],
    );
    return (results.rowCount ?? 0) > 0;
  }

  async markJobFinished(
    state: JobState.Succeeded | JobState.Failed | JobState.Aborted,
    jobId: JobId,
    attempt: number,
    result: JobResult,
  ): Promise<boolean> {
    const results = await this._pool.query<UpdateResult>(
      `UPDATE ${JOB_TABLE}
        SET result = $1,
            state = $2,
            progress = COALESCE($3, progress),
            finished_at = NOW()
      WHERE id = $4
        AND state = '${JobState.Running}'
        AND attempt = $5
      RETURNING max_attempts AS "maxAttempts"`,
      [JSON.stringify(result), state, state === JobState.Succeeded ? 1.0 : null, jobId, attempt],
    );

    // Unable to update row? (reasons: job has already been marked as finished, retried by a different worker, or record is gone)
    return (results.rowCount ?? 0) > 0;
  }

  async assignNextJob(
    workerId: WorkerId,
    taskNames: string[],
    timeout: number,
    options: JobDequeueOptions,
  ): Promise<JobDescriptor | null> {
    for (let repeat = 1; ; repeat--) {
      const dequeueJobInfo = await this.tryAssignNextJob(workerId, taskNames, options);
      if (dequeueJobInfo !== null) return dequeueJobInfo;
      if (timeout === 0 || repeat <= 0) return null;
      await this.waitForNewJobs(timeout);
    }
  }

  protected async tryAssignNextJob(
    workerId: WorkerId,
    taskNames: string[],
    options: JobDequeueOptions,
  ): Promise<JobDescriptor | null> {
    const jobId = options.id;
    const minPriority = options.minPriority;
    const queueNames = options.queueNames;

    const results = await this._pool.query<JobDescriptor>(
      `UPDATE ${JOB_TABLE}
      SET state = '${JobState.Running}',
          progress = 0.0,
          worker_id = $1,
          started_at = NOW()
      WHERE id = (
        SELECT id FROM ${JOB_TABLE} AS j
        WHERE id = COALESCE($2, id)
          AND queue_name = ANY ($3)
          AND task_name = ANY ($4)
          AND state IN ('${JobState.Pending}', '${JobState.Scheduled}')
          AND priority >= COALESCE($5, priority)
          AND (
            parent_job_ids = '{}'
            OR NOT EXISTS (
              SELECT 1 FROM ${JOB_TABLE}
              WHERE id = ANY (j.parent_job_ids)
                AND (
                  (state IN ('${JobState.Pending}',
                             '${JobState.Scheduled}') AND (expires_at IS NULL OR expires_at > NOW()))
                  OR state = '${JobState.Running}'
                  OR (state IN ('${JobState.Failed}',
                                '${JobState.Aborted}',
                                '${JobState.Abandoned}',
                                '${JobState.Unattended}',
                                '${JobState.Canceled}') AND NOT j.lax_dependency)
                )
            )
          )
          AND delay_until <= NOW()
          AND (expires_at IS NULL OR expires_at > NOW()
        )
        ORDER BY priority DESC, id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING id, task_name AS "taskName", args, max_attempts AS "maxAttempts", attempt`,
      [workerId, jobId, queueNames, taskNames, minPriority],
    );
    if ((results.rowCount ?? 0) <= 0) return null;

    return results.rows[0] ?? null;
  }

  /**
   * Wait a given amount of time for a new job to become available.
   */
  protected async waitForNewJobs(timeout: number): Promise<boolean> {
    const schema = await this.getSchema();

    const conn = await this._pool.connect();
    try {
      await conn.query(`LISTEN "${JOB_NOTIFICATION_CHANNEL}"`);
      await waitForPostgresNotification(conn, JOB_NOTIFICATION_CHANNEL, schema, timeout);
      return true;
    } catch (_: any) {
      return false;
    } finally {
      await conn.query(`UNLISTEN "${JOB_NOTIFICATION_CHANNEL}"`);
      conn.release();
    }
  }

  async removeJob(id: JobId): Promise<boolean> {
    const results = await this._pool.query(
      `DELETE FROM ${JOB_TABLE}
      WHERE id = $1
        AND state != '${JobState.Running}'
      `,
      [id],
    );
    return (results.rowCount ?? 0) > 0;
  }

  async pruneJobs(unattendedPeriod: number, expungePeriod: number, excludeQueues: string[]): Promise<JobPruneResult> {
    // Delete `pending`/`scheduled` jobs past expiration date
    const expiredJobsResult = await this._pool.query<JobDescriptor>(
      `DELETE FROM ${JOB_TABLE}
      WHERE state IN ('${JobState.Pending}', '${JobState.Scheduled}')
        AND expires_at <= NOW()
      RETURNING id, task_name AS "taskName", args, max_attempts AS "maxAttempts", attempt`,
    );

    // Delete `succeeded` jobs after the expunge period
    const expungedJobsResult = await this._pool.query<JobDescriptor>(
      `DELETE FROM ${JOB_TABLE}
      WHERE state = '${JobState.Succeeded}'
        AND NOW() - finished_at >= $1 * INTERVAL '1 millisecond'
      RETURNING id, task_name AS "taskName", args, max_attempts AS "maxAttempts", attempt`,
      [expungePeriod],
    );

    // Mark `pending`/`scheduled` jobs as `unattended` if they are due, but in the queue past `unattendedPeriod`.
    const unattendedJobsResult = await this._pool.query<JobDescriptor>(
      `UPDATE ${JOB_TABLE} SET
        state = '${JobState.Unattended}',
        result = '"Job appears unattended"'
      WHERE state IN ('${JobState.Pending}', '${JobState.Scheduled}')
        AND NOW() - delay_until > $1 * INTERVAL '1 millisecond'
      RETURNING id, task_name AS "taskName", args, max_attempts AS "maxAttempts", attempt`,
      [unattendedPeriod],
    );

    // Mark `running` jobs as `abandoned` if they are assigned to an `offline`, `lost`, or non-existing worker.
    const abandonedJobsResult = await this._pool.query<JobDescriptor & { workerId: WorkerId }>(
      `UPDATE ${JOB_TABLE} AS j
      SET result = $1,
          state = '${JobState.Abandoned}',
          finished_at = NOW()
      WHERE state = '${JobState.Running}'
        AND (queue_name != ANY ($2) OR $2 IS NULL)
        AND NOT EXISTS (
          SELECT 1
          FROM ${WORKER_TABLE}
          WHERE id = j.worker_id
            AND state IN ('${WorkerState.Online}', '${WorkerState.Busy}', '${WorkerState.Idle}')
        )
      RETURNING id, task_name AS "taskName", args, max_attempts AS "maxAttempts", attempt, worker_id AS "workerId"`,
      [JSON.stringify('Worker went away'), excludeQueues],
    );

    return {
      expiredJobs: expiredJobsResult.rows,
      expungedJobs: expungedJobsResult.rows,
      abandonedJobs: abandonedJobsResult.rows,
      unattendedJobs: unattendedJobsResult.rows,
    };
  }

  /**
   * Returns the information about jobs in batches.
   */
  async getJobInfos<A extends JobArgs>(
    offset: number,
    limit: number,
    options: ListJobsOptions,
  ): Promise<JobInfoList<A>> {
    const results = await this._pool.query<JobInfoRow<A>>(
      `SELECT
        id,

        task_name AS "taskName",
        queue_name AS "queueName",
        args,
        result,

        state,
        priority,
        progress,
        max_attempts AS "maxAttempts",
        attempt,

        parent_job_ids AS "parentJobIds",
        ARRAY(SELECT id FROM ${JOB_TABLE} WHERE parent_job_ids @> ARRAY[j.id]) AS "childJobIds",
        lax_dependency AS "laxDependency",

        worker_id AS "workerId",
        metadata,

        delay_until AS "delayUntil",
        started_at AS "startedAt",
        retried_at AS "retriedAt",
        finished_at AS "finishedAt",

        created_at AS "createdAt",
        expires_at AS "expiresAt",

        NOW() AS "time",
        COUNT(*) OVER() AS "total"
      FROM ${JOB_TABLE} AS j
      WHERE (id > $1 OR $1 IS NULL)
        AND (id = ANY ($2) OR $2 IS NULL)
        AND (queue_name = ANY ($3) OR $3 IS NULL)
        AND (task_name = ANY ($4) OR $4 IS NULL)
        AND (state = ANY ($5) OR $5 IS NULL)
        AND (metadata ? ANY ($6) OR $6 IS NULL)
        AND (state NOT IN ('${JobState.Pending}', '${JobState.Scheduled}') OR expires_at IS NULL OR expires_at > NOW())
      ORDER BY id ASC
      LIMIT $7 OFFSET $8`,
      [
        options.afterId,
        options.ids,
        options.queueNames,
        options.taskNames,
        options.states,
        options.metadata,
        limit,
        offset,
      ],
    );
    const total = removeTotal(results.rows);
    return { total, jobs: results.rows };
  }

  async getJobHistory(): Promise<QueueJobStatistics> {
    const results = await this._pool.query<DailyJobHistory>(
      `SELECT
        EXTRACT(EPOCH FROM ts) AS "epoch",
        COALESCE(succeeded_jobs, 0) AS "succeededJobs",
        COALESCE(failed_jobs, 0) AS "failedJobs",
        COALESCE(aborted_jobs, 0) AS "abortedJobs",
        COALESCE(abandoned_jobs, 0) AS "abandonedJobs",
        COALESCE(unattended_jobs, 0) AS "unattendedJobs"
      FROM
      (
        SELECT
          EXTRACT(DAY FROM ts) AS day,
          EXTRACT(HOUR FROM ts) AS hour,
          ts
        FROM GENERATE_SERIES(NOW() - INTERVAL '23 hour', NOW(), '1 hour') AS ts
      ) AS s
      LEFT JOIN
      (
        SELECT
          EXTRACT(DAY FROM finished_at) AS day,
          EXTRACT(HOUR FROM finished_at) AS hour,
          COUNT(*) FILTER (WHERE state = '${JobState.Succeeded}') AS succeeded_jobs,
          COUNT(*) FILTER (WHERE state = '${JobState.Failed}') AS failed_jobs,
          COUNT(*) FILTER (WHERE state = '${JobState.Aborted}') AS aborted_jobs,
          COUNT(*) FILTER (WHERE state = '${JobState.Abandoned}') AS abandoned_jobs,
          COUNT(*) FILTER (WHERE state = '${JobState.Unattended}') AS unattended_jobs
        FROM ${JOB_TABLE}
        WHERE finished_at > NOW() - INTERVAL '23 hours'
        GROUP BY day, hour
      ) AS j
      ON s.day = j.day AND s.hour = j.hour
      ORDER BY epoch ASC`,
    );
    return { daily: results.rows };
  }

  async registerWorker(options: WorkerRegistrationOptions): Promise<WorkerId> {
    const results = await this._pool.query<RegisterWorkerResult>(
      `INSERT INTO ${WORKER_TABLE} (
        config,
        state,
        host,
        pid,
        finished_job_count,
        metadata
      )
      VALUES (
        $1, $2, $3, $4, $5, $6
      )
      RETURNING id`,
      [options.config, options.state, this.hostname, process.pid, options.finishedJobCount, options.metadata],
    );
    return results.rows[0].id;
  }

  async updateWorker(workerId: WorkerId, options: WorkerRegistrationOptions): Promise<boolean> {
    const results = await this._pool.query<ReceiveResult>(
      `UPDATE ${WORKER_TABLE} AS new
      SET
        config = $1,
        state = $2,
        finished_job_count = $3,
        metadata = $4,
        last_seen_at = NOW()
      WHERE id = $5`,
      [options.config, options.state, options.finishedJobCount, options.metadata, workerId],
    );
    return (results.rowCount ?? 0) <= 0;
  }

  async checkWorkerInbox(workerId: WorkerId, options: WorkerInboxOptions): Promise<WorkerCommandDescriptor[]> {
    const results = await this._pool.query<ReceiveResult>(
      `UPDATE ${WORKER_TABLE} AS new
      SET
        state = $1,
        finished_job_count = $2,
        inbox = '[]',
        last_seen_at = NOW()
      FROM (
        SELECT id, inbox
        FROM ${WORKER_TABLE}
        WHERE id = $3
        FOR UPDATE
      ) AS old
      WHERE new.id = old.id
      RETURNING old.inbox AS "inbox"`,
      [options.state, options.finishedJobCount, workerId],
    );
    if ((results.rowCount ?? 0) <= 0) return [];
    return results.rows[0].inbox ?? [];
  }

  async unregisterWorker(id: WorkerId): Promise<boolean> {
    const results = await this._pool.query(
      `UPDATE ${WORKER_TABLE} SET state = '${WorkerState.Offline}' WHERE id = $1`,
      [id],
    );
    return (results.rowCount ?? 0) > 0;
  }

  async pruneWorkers(lostTimeout: number): Promise<WorkerPruneResult> {
    const lostWorkersResult = await this._pool.query<WorkerInfo>(
      `UPDATE ${WORKER_TABLE}
      SET state = '${WorkerState.Lost}'
      WHERE state IN ('${WorkerState.Online}', '${WorkerState.Idle}', '${WorkerState.Busy}')
        AND NOW() - last_seen_at > $1 * INTERVAL '1 millisecond'
      RETURNING
        id,
        config,
        state,
        host,
        pid,
        finished_job_count AS "finishedJobCount",
        metadata,
        started_at AS "startedAt",
        last_seen_at AS "lastSeenAt",
        '[]'::JSONB AS "jobs"`,
      [lostTimeout],
    );

    return {
      lostWorkers: lostWorkersResult.rows,
    };
  }

  async getWorkerInfo(workerId: WorkerId): Promise<WorkerInfo | undefined> {
    const results = await this._pool.query<WorkerInfoRow>(
      `SELECT
        id,

        config,
        state,
        host,
        pid,

        finished_job_count AS "finishedJobCount",
        metadata,

        started_at AS "startedAt",
        last_seen_at AS "lastSeenAt",

        ARRAY(
          SELECT id
          FROM ${JOB_TABLE}
          WHERE state = '${JobState.Running}'
            AND worker_id = w.id
        ) AS "jobs"
      FROM ${WORKER_TABLE} w
      WHERE id = $1`,
      [workerId],
    );
    return results.rows[0];
  }

  async getWorkerInfos(offset: number, limit: number, options: ListWorkersOptions): Promise<WorkerInfoList> {
    const results = await this._pool.query<WorkerInfoRow>(
      `SELECT
        id,

        config,
        state,
        host,
        pid,

        finished_job_count AS "finishedJobCount",
        metadata,

        started_at AS "startedAt",
        last_seen_at AS "lastSeenAt",

        ARRAY(
          SELECT id
          FROM ${JOB_TABLE}
          WHERE state = '${JobState.Running}'
            AND worker_id = w.id
        ) AS "jobs",
        COUNT(*) OVER() AS "total"
      FROM ${WORKER_TABLE} w
      WHERE (id > $1 OR $1 IS NULL)
        AND (id = ANY ($2) OR $2 IS NULL)
        AND (state = ANY ($3) OR $3 IS NULL)
        AND (metadata ? ANY ($4) OR $4 IS NULL)
      ORDER BY id ASC
      LIMIT $5 OFFSET $6`,
      [options.afterId, options.ids, options.state, options.metadata, limit, offset],
    );
    const total = removeTotal(results.rows);
    return { total, workers: results.rows };
  }

  async sendWorkerCommand(command: string, arg: WorkerCommandArg, options: ListWorkersOptions): Promise<boolean> {
    const descriptor: WorkerCommandDescriptor = { command, arg };
    const results = await this._pool.query(
      `UPDATE ${WORKER_TABLE} SET inbox = inbox || $1::JSONB
      WHERE (id > $2 OR $2 IS NULL)
        AND (id = ANY ($3) OR $3 IS NULL)
        AND (state = ANY ($4) OR $4 IS NULL)
        AND (metadata ? ANY ($5) OR $5 IS NULL)`,
      [JSON.stringify([descriptor]), options.afterId, options.ids, options.state, options.metadata],
    );
    return (results.rowCount ?? 0) > 0;
  }

  async getStats(): Promise<QueueStats> {
    const results = await this._pool.query<QueueStats>(
      `SELECT
        (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM ${JOB_TABLE}_id_seq) AS "enqueuedJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state IN ('${JobState.Pending}', '${JobState.Scheduled}') AND (expires_at IS NULL OR expires_at > NOW())) AS "pendingJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state = '${JobState.Scheduled}' AND delay_until > NOW()) AS "scheduledJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state = '${JobState.Running}')    AS "runningJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state = '${JobState.Succeeded}')  AS "succeededJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state = '${JobState.Failed}')     AS "failedJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state = '${JobState.Aborted}')    AS "abortedJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state = '${JobState.Abandoned}')  AS "abandonedJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state = '${JobState.Unattended}') AS "unattendedJobs",
        (SELECT COUNT(*) FROM ${JOB_TABLE} WHERE state = '${JobState.Canceled}')   AS "canceledJobs",

        (SELECT COUNT(*) FROM ${WORKER_TABLE} WHERE state = '${WorkerState.Offline}') AS "offlineWorkers",
        (SELECT COUNT(*) FROM ${WORKER_TABLE} WHERE state IN ('${WorkerState.Online}', '${WorkerState.Idle}', '${WorkerState.Busy}')) AS "onlineWorkers",
        (SELECT COUNT(DISTINCT worker_id) FROM ${JOB_TABLE} mj WHERE state = '${JobState.Running}') AS "busyWorkers",
        (SELECT COUNT(*) FROM ${WORKER_TABLE} WHERE state = '${WorkerState.Lost}') AS "lostWorkers",

        CURRENT_SETTING('server_version_num') AS "backendVersion",
        EXTRACT(EPOCH FROM NOW() - PG_POSTMASTER_START_TIME()) AS "backendUptime"`,
    );
    const stats = results.rows[0];

    stats.idleWorkers = stats.onlineWorkers - stats.busyWorkers;
    stats.backendVersion = stats.backendVersion.replace(/(^\d+)(?:0(\d)|([1-9]\d))(?:0(\d)|([1-9]\d))/, '$1.$2$3.$4$5');

    return stats;
  }

  async updateSchema(): Promise<void> {
    const version = (await this._pool.query<ServerVersionResult>('SHOW server_version_num')).rows[0].server_version_num;
    if (version < 90500) throw new Error('PostgreSQL 9.5 or later is required');

    const conn = await this._pool.connect();
    try {
      const migration = new Migration('queue', queueDatabaseUpgrades, conn);
      await migration.migrate();
    } finally {
      conn.release();
    }
  }

  async reset(): Promise<void> {
    await this._pool.query(`TRUNCATE ${JOB_TABLE}, ${WORKER_TABLE} RESTART IDENTITY`);
  }

  async end(): Promise<void> {
    if (this.autoclosePool) await this._pool.end();
  }
}

function removeTotal<T extends Array<{ total?: number }>>(results: T): number {
  let total = 0;
  for (const result of results) {
    if (result.total !== undefined) total = result.total;
    delete result.total;
  }
  return total;
}

/**
 * Waits for a PostgreSQL notification on the given channel, while expecting a specific payload. Returns
 * normally if notification was received, throws on timeout.
 */
async function waitForPostgresNotification(
  conn: pg.ClientBase,
  channel: string,
  expectedPayload: string,
  timeout: number,
): Promise<void> {
  let resolveFn = () => {};
  const handler = (notification: pg.Notification) => {
    if (notification.channel === channel && notification.payload === expectedPayload) resolveFn();
  };
  conn.on('notification', handler);

  try {
    let timer;
    const timeoutPromise = new Promise((_, rej) => (timer = setTimeout(rej, timeout)));

    const notifyPromise = new Promise<void>((res) => {
      resolveFn = res;
    });

    await Promise.race([notifyPromise, timeoutPromise]);
    clearTimeout(timer);
  } finally {
    conn.removeListener('notification', handler);
  }
}

const queueDatabaseUpgrades: MigrationStep[] = [
  {
    version: 1,
    sql: `
      CREATE TYPE ${JOB_TABLE}_state AS ENUM (
        '${JobState.Pending}',
        '${JobState.Scheduled}',
        '${JobState.Running}',
        '${JobState.Succeeded}',
        '${JobState.Failed}',
        '${JobState.Aborted}',
        '${JobState.Abandoned}',
        '${JobState.Unattended}',
        '${JobState.Canceled}'
      );

      CREATE TABLE ${JOB_TABLE} (
        id             BIGSERIAL NOT NULL PRIMARY KEY,

        queue_name     TEXT NOT NULL,
        task_name      TEXT NOT NULL,
        args           JSONB NOT NULL CHECK(JSONB_TYPEOF(args) = 'object'),
        result         JSONB,

        state          ${JOB_TABLE}_state NOT NULL,
        priority       INT NOT NULL,
        progress       REAL,
        max_attempts   INT NOT NULL,
        attempt        INT NOT NULL,

        parent_job_ids BIGINT[] NOT NULL DEFAULT '{}',
        lax_dependency BOOL NOT NULL DEFAULT FALSE,

        worker_id      BIGINT,
        metadata       JSONB CHECK(JSONB_TYPEOF(metadata) = 'object') NOT NULL DEFAULT '{}',

        delay_until    TIMESTAMP WITH TIME ZONE NOT NULL,
        started_at     TIMESTAMP WITH TIME ZONE,
        retried_at     TIMESTAMP WITH TIME ZONE,
        finished_at    TIMESTAMP WITH TIME ZONE,

        created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        expires_at     TIMESTAMP WITH TIME ZONE,
        CONSTRAINT state_pending_scheduled CHECK (
          NOT (state = 'pending' AND delay_until > NOW())
        )
      );
      CREATE INDEX ON ${JOB_TABLE} (state, priority DESC, id);
      CREATE INDEX ON ${JOB_TABLE} USING GIN (parent_job_ids);
      CREATE INDEX ON ${JOB_TABLE} USING GIN (metadata);
      CREATE INDEX ON ${JOB_TABLE} (expires_at);
      CREATE INDEX ON ${JOB_TABLE} (finished_at, state);
      CREATE FUNCTION ${JOB_NOTIFICATION_FUNCTION}() RETURNS trigger AS $$
        BEGIN
          IF new.delay_until <= NOW() THEN
            PERFORM pg_notify('${JOB_NOTIFICATION_CHANNEL}', current_schema);
          END IF;
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
      CREATE TRIGGER ${JOB_NOTIFICATION_TRIGGER}
        AFTER INSERT OR UPDATE OF attempt ON ${JOB_TABLE}
        FOR EACH ROW EXECUTE PROCEDURE ${JOB_NOTIFICATION_FUNCTION}();

      CREATE TYPE ${WORKER_TABLE}_state AS ENUM (
        '${WorkerState.Offline}',
        '${WorkerState.Online}',
        '${WorkerState.Idle}',
        '${WorkerState.Busy}',
        '${WorkerState.Lost}'
      );
      CREATE UNLOGGED TABLE ${WORKER_TABLE} (
        id                 BIGSERIAL NOT NULL PRIMARY KEY,

        config             JSONB CHECK(JSONB_TYPEOF(config) = 'object') NOT NULL DEFAULT '{}',
        state              ${WORKER_TABLE}_state NOT NULL,
        host               TEXT NOT NULL,
        pid                INT NOT NULL,

        finished_job_count BIGINT NOT NULL DEFAULT 0,
        metadata           JSONB CHECK(JSONB_TYPEOF(metadata) = 'object') NOT NULL DEFAULT '{}',
        inbox              JSONB CHECK(JSONB_TYPEOF(inbox) = 'array') NOT NULL DEFAULT '[]',

        started_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        last_seen_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
      );
      `,
  },
];

interface EnqueueResult {
  id: JobId;
}

interface JobInfoRow<A extends JobArgs> extends JobInfo<A> {
  total: number;
}

interface WorkerInfoRow extends WorkerInfo {
  total: number;
}

interface ReceiveResult {
  inbox: WorkerCommandDescriptor[];
}

interface RegisterWorkerResult {
  id: WorkerId;
}

interface ServerVersionResult {
  server_version_num: number;
}

interface UpdateResult {
  maxAttempts: number;
}
