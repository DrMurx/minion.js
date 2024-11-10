import EventEmitter from 'events';
import os from 'os';
import pg, { QueryConfigValues, QueryResult, QueryResultRow } from 'pg';
import {
  type Backend,
  type JobDequeueOptions,
  type JobEnqueueOptions,
  type JobInfoList,
  type JobOptions,
  type JobPruneResult,
  type WorkerInfoList,
  type WorkerPruneResult,
  type WorkerRegistrationOptions,
  type WorkerUpdateOptions,
} from '../../types/backend.js';
import {
  type JobArgs,
  type JobDescriptor,
  type JobId,
  type JobInfo,
  type JobResult,
  JobState,
  type ListJobsOptions,
} from '../../types/job.js';
import { type DailyJobHistory, type QueueJobStatistics, type QueueStats } from '../../types/queue-stats.js';
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
    } else if (typeof config === 'string') {
      this._pool = createPool(config);
      this.autoclosePool = true;
    } else {
      throw new Error('Invalid config for PgBackend');
    }
  }

  get pool(): pg.Pool {
    return this._pool;
  }

  protected query<R extends QueryResultRow = any>(
    sql: string,
    values?: QueryConfigValues<any>,
  ): Promise<QueryResult<R>> {
    return this._pool.query<R>(sql, values);
  }

  protected async getSchema(): Promise<string> {
    if (this._schema) return this._schema;
    const results = await this.query<{ current_schema: string }>(`SELECT current_schema`);
    return (this._schema = results.rows[0].current_schema);
  }

  async addJob<Args extends JobArgs>(taskName: string, args: Args, options: JobEnqueueOptions): Promise<JobInfo<Args>> {
    const results = await this.query<JobInfo<Args>>(
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
      RETURNING ${this.jobInfoSql}`,
      [
        options.queueName,
        taskName,
        JSON.stringify(args),
        options.delayFor <= 0 ? JobState.Pending : JobState.Scheduled,
        options.priority,
        options.maxAttempts,
        1,
        options.parentJobIds,
        options.laxDependency,
        options.metadata,
        options.delayFor,
        options.expireIn,
      ],
    );

    const jobInfo = results.rows[0];
    if (jobInfo !== undefined) {
      if (jobInfo.parentJobIds.length > 0) jobInfo.parentJobIds = jobInfo.parentJobIds.map(Number);
    }
    return jobInfo;
  }

  async retryJob<Args extends JobArgs>(
    id: JobId,
    attempt: number,
    options: JobOptions,
  ): Promise<JobInfo<Args> | undefined> {
    const delayFor = options.delayFor ?? 0;
    const results = await this.query<JobInfo<Args>>(
      `UPDATE ${JOB_TABLE} SET
        queue_name = COALESCE($1, queue_name),
        state = $2,
        priority = COALESCE($3, priority),
        progress = 0.0,
        max_attempts = COALESCE($4, max_attempts + 1),
        attempt = attempt + 1,
        parent_job_ids = COALESCE($5, parent_job_ids),
        lax_dependency = COALESCE($6, lax_dependency),
        metadata = JSONB_STRIP_NULLS(metadata || $7),
        delay_until = NOW() + $8 * INTERVAL '1 millisecond',
        retried_at = NOW(),
        expires_at = CASE WHEN $9::BIGINT IS NULL THEN expires_at ELSE NOW() + $9::BIGINT * INTERVAL '1 millisecond' END
      WHERE id = $10
        AND attempt = $11
      RETURNING ${this.jobInfoSql}`,
      [
        options.queueName,
        delayFor <= 0 ? JobState.Pending : JobState.Scheduled,
        options.priority,
        options.maxAttempts,
        options.parentJobIds,
        options.laxDependency,
        options.metadata ?? {},
        delayFor,
        options.expireIn,
        id,
        attempt,
      ],
    );

    const jobInfo = results.rows[0];
    if (jobInfo !== undefined) {
      if (jobInfo.parentJobIds.length > 0) jobInfo.parentJobIds = jobInfo.parentJobIds.map(Number);
    }
    return jobInfo;
  }

  async cancelJob(id: JobId): Promise<boolean> {
    const results = await this.query(
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

  async amendJobMetadata(
    id: JobId,
    attempt: number,
    records: Record<string, any>,
  ): Promise<Record<string, any> | undefined> {
    const results = await this.query<{ metadata: Record<string, any> }>(
      `UPDATE ${JOB_TABLE}
      SET metadata = JSONB_STRIP_NULLS(metadata || $1)
      WHERE id = $2
        AND attempt = $3
      RETURNING metadata`,
      [records, id, attempt],
    );
    const jobInfo = results.rows[0];
    return jobInfo !== undefined ? jobInfo.metadata : undefined;
  }

  async updateJobProgress(id: JobId, attempt: number, progress: number): Promise<boolean> {
    const results = await this.query<{ workerId: WorkerId }>(
      `UPDATE ${JOB_TABLE}
      SET progress = $1
      WHERE id = $2
        AND attempt = $3
      RETURNING worker_id AS workerId`,
      [progress, id, attempt],
    );
    if (results.rowCount === 0) return false;
    // if (results.rows[0].workerId) {
    //   this.emit('worker_', { workerId: results.rows[0].workerId });
    // }
    return true;
  }

  async markJobFinished(
    jobId: JobId,
    attempt: number,
    state: JobState.Succeeded | JobState.Failed | JobState.Aborted,
    result: JobResult,
  ): Promise<boolean> {
    const results = await this.query(
      `UPDATE ${JOB_TABLE}
        SET result = $1,
            state = $2,
            progress = COALESCE($3, progress),
            finished_at = NOW()
      WHERE id = $4
        AND state = '${JobState.Running}'
        AND attempt = $5`,
      [JSON.stringify(result), state, state === JobState.Succeeded ? 1.0 : null, jobId, attempt],
    );

    // Unable to update row? (reasons: job has already been marked as finished, retried by a different worker, or record is gone)
    return (results.rowCount ?? 0) > 0;
  }

  async assignNextJob<Args extends JobArgs>(
    workerId: WorkerId,
    taskNames: string[],
    timeout: number,
    options: JobDequeueOptions,
  ): Promise<JobInfo<Args> | null> {
    for (let repeat = 1; ; repeat--) {
      const dequeueJobInfo = await this.tryAssignNextJob<Args>(workerId, taskNames, options);
      if (dequeueJobInfo !== null) return dequeueJobInfo;
      if (timeout === 0 || repeat <= 0) return null;
      await this.waitForNewJobs(timeout);
    }
  }

  protected async tryAssignNextJob<Args extends JobArgs>(
    workerId: WorkerId,
    taskNames: string[],
    options: JobDequeueOptions,
  ): Promise<JobInfo<Args> | null> {
    const jobId = options.id;
    const minPriority = options.minPriority;
    const queueNames = Array.isArray(options.queueNames) ? options.queueNames : [options.queueNames];

    const results = await this.query<JobInfo<Args>>(
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
      RETURNING ${this.jobInfoSql}`,
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
    const results = await this.query(
      `DELETE FROM ${JOB_TABLE}
      WHERE id = $1
        AND state != '${JobState.Running}'
      `,
      [id],
    );
    return (results.rowCount ?? 0) > 0;
  }

  async pruneJobs<Args extends JobArgs>(
    unattendedPeriod: number,
    expungePeriod: number,
    ignoreQueues: string[],
  ): Promise<JobPruneResult<Args>> {
    // Delete `pending`/`scheduled` jobs past expiration date
    const expiredJobsResult = await this.query<JobDescriptor<Args>>(
      `DELETE FROM ${JOB_TABLE}
      WHERE state IN ('${JobState.Pending}', '${JobState.Scheduled}')
        AND expires_at <= NOW()
      RETURNING ${this.jobDescriptorSql}`,
    );

    // Delete `succeeded` jobs after the expunge period
    const expungedJobsResult = await this.query<JobInfo<Args>>(
      `DELETE FROM ${JOB_TABLE}
      WHERE state = '${JobState.Succeeded}'
        AND NOW() - finished_at >= $1 * INTERVAL '1 millisecond'
      RETURNING ${this.jobInfoSql}`,
      [expungePeriod],
    );

    // Mark `pending`/`scheduled` jobs as `unattended` if they are due, but in the queue past `unattendedPeriod`.
    const unattendedJobsResult = await this.query<JobDescriptor<Args>>(
      `UPDATE ${JOB_TABLE} SET
        state = '${JobState.Unattended}'
      WHERE state IN ('${JobState.Pending}', '${JobState.Scheduled}')
        AND NOW() - delay_until > $1 * INTERVAL '1 millisecond'
      RETURNING ${this.jobDescriptorSql}`,
      [unattendedPeriod],
    );

    // Mark `running` jobs as `abandoned` if they are assigned to an `offline`, `lost`, or non-existing worker.
    const abandonedJobsResult = await this.query<JobInfo<Args>>(
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
      RETURNING ${this.jobInfoSql}`,
      [JSON.stringify({ name: 'WorkerGoneError', message: 'Worker went away' }), ignoreQueues],
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
  async getJobInfo<Args extends JobArgs>(jobId: JobId): Promise<JobInfo<Args> | undefined> {
    const results = await this.query<JobInfo<Args>>(
      `SELECT
        ${this.jobInfoSql},
        ARRAY(SELECT id FROM ${JOB_TABLE} WHERE parent_job_ids @> ARRAY[j.id]) AS "childJobIds",
        NOW() AS "time",
        COUNT(*) OVER() AS "total"
      FROM ${JOB_TABLE} AS j
      WHERE id = $1`,
      [jobId],
    );
    const jobInfo = results.rows[0];
    if (jobInfo !== undefined) {
      if (jobInfo.parentJobIds.length > 0) jobInfo.parentJobIds = jobInfo.parentJobIds.map(Number);
      if (jobInfo.childJobIds.length) jobInfo.childJobIds = jobInfo.childJobIds.map(Number);
    }
    return jobInfo;
  }

  async getJobInfos<Args extends JobArgs>(
    offset: number,
    limit: number,
    options: ListJobsOptions,
  ): Promise<JobInfoList<Args>> {
    const results = await this.query<JobInfo<Args> & { total: number }>(
      `SELECT
        ${this.jobInfoSql},
        ARRAY(SELECT id FROM ${JOB_TABLE} WHERE parent_job_ids @> ARRAY[j.id]) AS "childJobIds",
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
    results.rows.forEach((jobInfo) => {
      if (jobInfo.parentJobIds.length) jobInfo.parentJobIds = jobInfo.parentJobIds.map(Number);
      if (jobInfo.childJobIds.length) jobInfo.childJobIds = jobInfo.childJobIds.map(Number);
    });
    return { total, jobs: results.rows };
  }

  async registerWorker(options: WorkerRegistrationOptions): Promise<WorkerId> {
    const results = await this.query<{ id: WorkerId }>(
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

  async updateWorker(workerId: WorkerId, options: WorkerUpdateOptions): Promise<boolean> {
    const results = await this.query(
      `UPDATE ${WORKER_TABLE} AS new
      SET
        config = COALESCE($1, config),
        state = COALESCE($2, state),
        finished_job_count = COALESCE($3, finished_job_count),
        metadata = JSONB_STRIP_NULLS(metadata || $4),
        last_seen_at = NOW()
      WHERE id = $5`,
      [options.config, options.state, options.finishedJobCount, options.metadata ?? {}, workerId],
    );
    return (results.rowCount ?? 0) <= 0;
  }

  async checkWorkerInbox(workerId: WorkerId, options: WorkerUpdateOptions): Promise<WorkerCommandDescriptor[]> {
    const results = await this.query<{ inbox: WorkerCommandDescriptor[] }>(
      `UPDATE ${WORKER_TABLE} AS new
      SET
        config = COALESCE($1, config),
        state = COALESCE($2, state),
        finished_job_count = COALESCE($3, finished_job_count),
        metadata = JSONB_STRIP_NULLS(metadata || $4),
        inbox = '[]',
        last_seen_at = NOW()
      FROM (
        SELECT id, inbox
        FROM ${WORKER_TABLE}
        WHERE id = $5
        FOR UPDATE
      ) AS old
      WHERE new.id = old.id
      RETURNING old.inbox AS "inbox"`,
      [options.config, options.state, options.finishedJobCount, options.metadata ?? {}, workerId],
    );
    if ((results.rowCount ?? 0) <= 0) return [];
    return results.rows[0].inbox ?? [];
  }

  async unregisterWorker(id: WorkerId): Promise<boolean> {
    const results = await this.query(`UPDATE ${WORKER_TABLE} SET state = '${WorkerState.Offline}' WHERE id = $1`, [id]);
    return (results.rowCount ?? 0) > 0;
  }

  async pruneWorkers(lostTimeout: number): Promise<WorkerPruneResult> {
    const lostWorkersResult = await this.query<WorkerInfo>(
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
    const results = await this.query<WorkerInfo>(
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
        ) AS "jobIds"
      FROM ${WORKER_TABLE} w
      WHERE id = $1`,
      [workerId],
    );
    const workerInfo = results.rows[0];
    if (workerInfo.jobIds.length) workerInfo.jobIds = workerInfo.jobIds.map(Number);
    return workerInfo;
  }

  async getWorkerInfos(offset: number, limit: number, options: ListWorkersOptions): Promise<WorkerInfoList> {
    const results = await this.query<WorkerInfo & { total: number }>(
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
        ) AS "jobIds",
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
    results.rows.forEach((workerInfo) => {
      if (workerInfo.jobIds.length) workerInfo.jobIds = workerInfo.jobIds.map(Number);
    });
    return { total, workers: results.rows };
  }

  async sendWorkerCommand(command: string, arg: WorkerCommandArg, options: ListWorkersOptions): Promise<boolean> {
    const descriptor: WorkerCommandDescriptor = { command, arg };
    const results = await this.query(
      `UPDATE ${WORKER_TABLE} SET inbox = inbox || $1::JSONB
      WHERE (id > $2 OR $2 IS NULL)
        AND (id = ANY ($3) OR $3 IS NULL)
        AND (state = ANY ($4) OR $4 IS NULL)
        AND (metadata ? ANY ($5) OR $5 IS NULL)`,
      [JSON.stringify([descriptor]), options.afterId, options.ids, options.state, options.metadata],
    );
    return (results.rowCount ?? 0) > 0;
  }

  async getJobHistory(): Promise<QueueJobStatistics> {
    const results = await this.query<DailyJobHistory>(
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

  async getStats(): Promise<QueueStats> {
    const results = await this.query<QueueStats>(
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
    const result = await this.query<{ server_version_num: number }>('SHOW server_version_num');
    const version = result.rows[0].server_version_num;
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
    await this.query(`TRUNCATE ${JOB_TABLE}, ${WORKER_TABLE} RESTART IDENTITY`);
  }

  async end(): Promise<void> {
    if (this.autoclosePool) await this._pool.end();
  }

  protected get jobInfoSql() {
    return `id,

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
      lax_dependency AS "laxDependency",

      worker_id AS "workerId",
      metadata,

      delay_until AS "delayUntil",
      started_at AS "startedAt",
      retried_at AS "retriedAt",
      finished_at AS "finishedAt",

      created_at AS "createdAt",
      expires_at AS "expiresAt"`;
  }
  protected get jobDescriptorSql() {
    return `id,

      task_name AS "taskName",
      args,

      max_attempts AS "maxAttempts",
      attempt`;
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
        id             BIGSERIAL NOT NULL,

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
      ) PARTITION BY LIST (state);
      ALTER TABLE ${JOB_TABLE} ADD PRIMARY KEY (id, state);
      CREATE INDEX ON ${JOB_TABLE} (state, priority DESC, id);
      CREATE INDEX ON ${JOB_TABLE} USING GIN (parent_job_ids);
      CREATE INDEX ON ${JOB_TABLE} USING GIN (metadata);
      CREATE INDEX ON ${JOB_TABLE} (expires_at);
      CREATE INDEX ON ${JOB_TABLE} (finished_at, state);
      CREATE TABLE ${JOB_TABLE}_active PARTITION OF ${JOB_TABLE} FOR VALUES IN (
        '${JobState.Pending}',
        '${JobState.Scheduled}',
        '${JobState.Running}'
      );
      CREATE TABLE ${JOB_TABLE}_finished PARTITION OF ${JOB_TABLE} FOR VALUES IN (
        '${JobState.Succeeded}',
        '${JobState.Failed}',
        '${JobState.Aborted}',
        '${JobState.Abandoned}',
        '${JobState.Unattended}',
        '${JobState.Canceled}'
      );
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
