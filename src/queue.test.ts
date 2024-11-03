import t from 'tap';
import { JOB_TABLE, PgBackend, WORKER_TABLE } from './backends/pg/backend.js';
import { createPool } from './backends/pg/factory.js';
import { DefaultQueue } from './queue.js';
import { DefaultQueuedJob } from './queued-job.js';
import { type Backend } from './types/backend.js';
import { JobState } from './types/job.js';
import { type Queue } from './types/queue.js';
import { type Task } from './types/task.js';
import { WorkerState } from './types/worker.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};

t.test('Queue with PostgreSQL backend', skip, async (t) => {
  const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=queue_test`);

  // Isolate tests
  await pool.query('DROP SCHEMA IF EXISTS queue_test CASCADE');
  await pool.query('CREATE SCHEMA queue_test');

  const backend: Backend = new PgBackend(pool);
  const queue: Queue = new DefaultQueue(backend, {
    // Register at some simple tasks for further tests
    tasks: {
      fail: () => {
        throw new Error('Intentional failure!');
      },
    },
  });
  await queue.start();

  // Register at some simple tasks for further tests
  queue.registerTask(
    new (class implements Task {
      readonly name = 'test';
      async handle() {}
    })(),
  );
  queue.registerTask('add', async (job) => {
    const { first, second } = job.args as any;
    return { added: first + second };
  });

  await t.test('Nothing to prune initially', async (t) => {
    t.notOk(await queue.prune());
  });

  await t.test('Job results', async (t) => {
    const worker = await queue.getNewWorker().register();

    const queuedJob1 = await queue.addJob('test');
    const resultPromise1 = queue.getJobResult(queuedJob1.id, { interval: 0 });
    const executor1 = (await worker.getNextExecutor(0))!;
    const job1 = executor1.job;
    t.equal(job1.id, queuedJob1.id);
    t.same(job1.progress, 0.0);
    t.same(await job1.amendMetadata({ foo: 'bar' }), true);
    t.same(await executor1.markSucceeded({ just: 'works' }), true);
    t.same(job1.progress, 1.0);
    const result1 = (await resultPromise1)!;
    t.same(result1, { just: 'works' });
    t.ok(await queuedJob1.sync());
    t.same(queuedJob1.progress, 1.0);
    t.same(queuedJob1.metadata, { foo: 'bar' });

    let failed;
    const queuedJob2 = await queue.addJob('test');
    t.not(queuedJob2.id, queuedJob1.id);
    const promise2 = queue.getJobResult(queuedJob2.id, { interval: 0 }).catch((reason) => (failed = reason));
    const executor2 = (await worker.getNextExecutor())!;
    t.equal(executor2.id, queuedJob2.id);
    t.not(executor2.id, queuedJob1.id);
    t.same(await executor2.markFailed({ just: 'works too' }), true);
    await promise2;
    t.same(failed!.result, { just: 'works too' });

    const result2 = (await queue.getJobResult(queuedJob1.id, { interval: 0 }))!;
    t.same(result2, { just: 'works' });

    t.ok(await queuedJob1.sync());
    t.same(queuedJob1.progress, 1.0);
    t.same(queuedJob1.metadata, { foo: 'bar' });

    let succeeded;
    failed = undefined;
    const queuedJob1a = (await queue.getJob(queuedJob1.id))!;
    const queuedJob1b = await queuedJob1a.retry();
    t.ok(queuedJob1b instanceof DefaultQueuedJob);
    t.equal(queuedJob1b!.state, JobState.Pending);
    const ac = new AbortController();
    const signal = ac.signal;
    const promise4 = queue
      .getJobResult(queuedJob1.id, { interval: 10, signal })
      .then((value) => (succeeded = value))
      .catch((reason) => (failed = reason));
    setTimeout(() => ac.abort(), 250);
    await promise4;
    t.same(succeeded, undefined);
    t.same(failed!.name, 'AbortError');

    succeeded = undefined;
    failed = undefined;
    const job4 = (await queue.getJob(queuedJob1.id))!;
    t.same(await job4.remove(), true);
    const promise5 = queue
      .getJobResult(queuedJob1.id, { interval: 10, signal })
      .then((value) => (succeeded = value))
      .catch((reason) => (failed = reason));
    await promise5;
    t.same(succeeded, null);
    t.same(failed, undefined);

    await worker.unregister();
  });

  await t.test('Wait for job to be assigned to worker', async (t) => {
    const worker = await queue.getNewWorker().register();
    setTimeout(() => queue.addJob('test'), 500);
    const executor = (await worker.getNextExecutor(10000))!;
    t.notSame(executor, null);
    await executor.markSucceeded({ one: ['two', ['three']] });
    const queuedJob1 = (await queue.getJob(executor.id))!;
    t.same(queuedJob1.result, { one: ['two', ['three']] });
    await worker.unregister();
  });

  await t.test('Repair lost worker', async (t) => {
    const worker1 = await queue.getNewWorker().register();
    const worker2 = await queue.getNewWorker().register();
    t.not(worker1.id, worker2.id);

    const queuedJob1 = await queue.addJob('test');
    const job = (await worker2.getNextExecutor())!;
    t.equal(job.id, queuedJob1.id);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Running);
    const workerId = worker2.id;
    const lostAfter = DefaultQueue.DEFAULT_OPTIONS.workerLostTimeout + 1;
    t.ok(await worker2.getInfo());

    await pool.query(`UPDATE ${WORKER_TABLE} SET last_seen_at = NOW() - $1 * INTERVAL '1 millisecond' WHERE id = $2`, [
      lostAfter,
      workerId,
    ]);

    await queue.prune();
    t.equal((await worker2.getInfo())!.state, WorkerState.Lost);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Abandoned);
    t.same(queuedJob1.result, { name: 'WorkerGoneError', message: 'Worker went away' });
    t.equal((await queue.getStatistics()).abandonedJobs, 1);
    await worker1.unregister();
    await worker2.unregister();
  });

  await t.test('Repair abandoned job', async (t) => {
    const worker = await queue.getNewWorker().register();
    const queuedJob1 = await queue.addJob('test');
    (await worker.getNextExecutor())!;
    await worker.unregister();

    await queue.prune();

    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Abandoned);
    t.same(queuedJob1.result, { name: 'WorkerGoneError', message: 'Worker went away' });
    t.equal((await queue.getStatistics()).abandonedJobs, 2);
  });

  await t.test('Repair abandoned job in foreground queue (have to be handled manually)', async (t) => {
    const worker = await queue.getNewWorker().register();
    const queuedJob1 = await queue.addJob('test', {}, { queueName: backend.FOREGROUND_QUEUE });
    const job = (await worker.getNextExecutor(0, { queueNames: [backend.FOREGROUND_QUEUE] }))!;
    t.equal(job.id, queuedJob1.id);
    await worker.unregister();

    await queue.prune();

    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Running);
    t.equal(queuedJob1.queueName, backend.FOREGROUND_QUEUE);
    t.same(queuedJob1.result, null);
  });

  await t.test('Repair old jobs', async (t) => {
    const expungePeriod = DefaultQueue.DEFAULT_OPTIONS.jobExpungePeriod;
    t.equal(expungePeriod, 172800000);

    const worker = await queue.getNewWorker().register();
    const queuedJob1 = await queue.addJob('test');
    const queuedJob2 = await queue.addJob('test');
    const queuedJob3 = await queue.addJob('test');

    await worker.getNextExecutor().then((job) => job!.perform(worker));
    await worker.getNextExecutor().then((job) => job!.perform(worker));
    await worker.getNextExecutor().then((job) => job!.perform(worker));

    t.ok(await queuedJob2.sync());
    const finishedAt1 = queuedJob2.finishedAt!.getMilliseconds();
    await pool.query(`UPDATE ${JOB_TABLE} SET finished_at = TO_TIMESTAMP($1) WHERE id = $2`, [
      finishedAt1 - (expungePeriod + 1),
      queuedJob2.id,
    ]);
    t.ok(await queuedJob3.sync());
    const finishedAt2 = queuedJob3.finishedAt!.getMilliseconds();
    await pool.query(`UPDATE ${JOB_TABLE} SET finished_at = TO_TIMESTAMP($1) WHERE id = $2`, [
      finishedAt2 - (expungePeriod + 1),
      queuedJob3.id,
    ]);

    await worker.unregister();

    await queue.prune();

    t.ok(await queue.getJob(queuedJob1.id));
    t.notOk(await queue.getJob(queuedJob2.id));
    t.notOk(await queue.getJob(queuedJob3.id));
  });

  await t.test('Repair unattended jobs', async (t) => {
    t.equal(DefaultQueue.DEFAULT_OPTIONS.jobUnattendedPeriod, 172800000);

    const worker = await queue.getNewWorker().register();
    const queuedJob1 = await queue.addJob('test', { delayFor: 1000 });
    const queuedJob2 = await queue.addJob('test', { delayFor: 1000 });
    const queuedJob3 = await queue.addJob('test', { delayFor: 1000 });
    const queuedJob4 = await queue.addJob('test', { delayFor: 1000 });

    const unattendedPeriod = DefaultQueue.DEFAULT_OPTIONS.jobUnattendedPeriod + 1;
    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() - $1 * INTERVAL '1 second' WHERE id = ANY ($2)`, [
      unattendedPeriod,
      [queuedJob1.id, queuedJob2.id, queuedJob3.id, queuedJob4.id],
    ]);

    const job1 = (await worker.getNextExecutor(0, { id: queuedJob4.id }))!;
    await job1.markSucceeded({ i_say: 'Works!' });
    const job2 = (await worker.getNextExecutor(0, { id: queuedJob2.id }))!;
    await queue.prune();

    t.ok(await queuedJob2.sync());
    t.equal(queuedJob2.state, JobState.Running);
    t.ok(await job2.markSucceeded());

    t.equal((await queue.getStatistics()).unattendedJobs, 2);
    t.ok(await queuedJob1.sync());
    t.equal(queuedJob1.state, JobState.Unattended);
    t.ok(await queuedJob3.sync());
    t.equal(queuedJob3.state, JobState.Unattended);

    t.ok(await queuedJob4.sync());
    t.equal(queuedJob4.state, JobState.Succeeded);
    t.same(queuedJob4.result, { i_say: 'Works!' });

    await worker.unregister();
  });

  await t.test('List jobs', async (t) => {
    await queue.resetQueue();

    const worker = await queue.getNewWorker().register();
    const queuedJob1 = await queue.addJob('test');
    const queuedJob2 = await queue.addJob('test');
    const queuedJob3 = await queue.addJob('test');
    const queuedJob4 = await queue.addJob('test');
    const queuedJob5 = await queue.addJob('test');

    const job1 = (await worker.getNextExecutor(0))!;
    const job2 = (await worker.getNextExecutor(0))!;
    t.same(await job2.markSucceeded(), true);
    t.same(await job1.markSucceeded(), true);
    const job3 = (await worker.getNextExecutor(0))!;
    t.same(await job3.markFailed(), true);
    t.ok(await queuedJob3.retry());
    const job3a = (await worker.getNextExecutor(0))!;
    await job3a.markSucceeded({ it: 'works' });
    await worker.getNextExecutor(0);
    await worker.unregister();

    await t.test('Simple list with default chunk size', async (t) => {
      const jobs1 = queue.listJobInfos();
      t.equal(await jobs1.numRows(), 5);
      t.equal((await jobs1.next())!.id, queuedJob1.id);
      t.equal(jobs1.highestId, queuedJob5.id);
      t.equal((await jobs1.next())!.id, queuedJob2.id);
      t.equal((await jobs1.next())!.id, queuedJob3.id);
      t.equal((await jobs1.next())!.id, queuedJob4.id);
      t.equal((await jobs1.next())!.id, queuedJob5.id);
      t.notOk(await jobs1.next());
    });

    await t.test('List with filters', async (t) => {
      const jobs2 = queue.listJobInfos({ states: [JobState.Pending] });
      t.equal(await jobs2.numRows(), 1);
      t.equal((await jobs2.next())!.id, queuedJob5.id);
      t.notOk(await jobs2.next());

      const jobs3 = queue.listJobInfos({ states: [JobState.Running] });
      t.equal(await jobs3.numRows(), 1);
      t.equal((await jobs3.next())!.id, queuedJob4.id);
      t.notOk(await jobs3.next());
    });

    await t.test('List with small chunk size', async (t) => {
      const jobs4 = queue.listJobInfos({}, 2);
      t.notOk(jobs4.highestId);
      t.equal((await jobs4.next())!.id, queuedJob1.id);
      t.equal(jobs4.highestId, queuedJob2.id);
      t.equal((await jobs4.next())!.id, queuedJob2.id);
      t.equal(jobs4.highestId, queuedJob2.id);
      t.equal((await jobs4.next())!.id, queuedJob3.id);
      t.equal(jobs4.highestId, queuedJob4.id);
      t.equal((await jobs4.next())!.id, queuedJob4.id);
      t.equal(jobs4.highestId, queuedJob4.id);
      t.equal((await jobs4.next())!.id, queuedJob5.id);
      t.equal(jobs4.highestId, queuedJob5.id);
      t.notOk(await jobs4.next());
      t.equal(await jobs4.numRows(), 5);
    });
  });

  await t.test('Enqueue, dequeue and perform', async (t) => {
    await queue.resetQueue();

    t.notOk(await queue.getJob(12345));

    const queuedJob1 = await queue.addJob('add', { first: 2, second: 2 });
    t.same(queuedJob1.args, { first: 2, second: 2 });
    t.equal(queuedJob1.state, JobState.Pending);
    t.equal(queuedJob1.priority, 0);

    const worker = queue.getNewWorker();
    t.same(await worker.getNextExecutor(), null);

    await worker.register();
    const executor1 = (await worker.getNextExecutor())!;
    const job1 = executor1.job;
    t.same((await worker.getInfo())!.jobIds, [queuedJob1.id]);
    t.equal(job1.taskName, 'add');
    t.equal(job1.attempt, 1);
    t.same(executor1.job.args, { first: 2, second: 2 });
    t.ok(await queuedJob1.sync());
    t.equal(queuedJob1.state, JobState.Running);
    t.equal(queuedJob1.workerId, worker.id);
    t.same(queuedJob1.createdAt instanceof Date, true);
    t.same(queuedJob1.startedAt instanceof Date, true);
    t.notOk(queuedJob1.finishedAt);
    t.same(queuedJob1.time instanceof Date, true);

    await executor1.perform(worker);
    t.same((await worker.getInfo())!.jobIds, []);
    t.ok(await queuedJob1.sync());
    t.equal(queuedJob1.state, JobState.Succeeded);
    t.same(queuedJob1.result, { added: 4 });
    t.same(queuedJob1.finishedAt instanceof Date, true);
    await worker.unregister();

    const queuedJob1b = (await queue.getJob(executor1.id))!;
    t.same(queuedJob1b.taskName, 'add');
    t.same(queuedJob1b.args, { first: 2, second: 2 });
    t.equal(queuedJob1b.state, JobState.Succeeded);
  });

  await t.test('Cancel job', async (t) => {
    const worker = await queue.getNewWorker().register();

    const queuedJob1 = await queue.addJob('add', { first: 11, second: 17 }, { delayFor: 10000 });
    t.notOk(await worker.getNextExecutor());
    t.equal(queuedJob1.state, JobState.Scheduled);
    t.ok(await queuedJob1.cancel());
    t.equal(queuedJob1.state, JobState.Canceled);

    const queuedJob2 = await queue.addJob('add', { first: 13, second: 29 });
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, queuedJob2.id);
    await queuedJob2.sync();
    t.equal(queuedJob2.state, JobState.Running);
    t.notOk(await queuedJob2.cancel());
    await queuedJob2.sync();
    t.equal(queuedJob2.state, JobState.Running);

    const queuedJob3 = await queue.addJob('add', { first: 17, second: 29 });
    const job3 = (await worker.getNextExecutor())!;
    t.equal(job3.id, queuedJob3.id);
    await job3.markSucceeded();
    await queuedJob3.sync();
    t.equal(queuedJob3.state, JobState.Succeeded);
    t.notOk(await queuedJob3.cancel());
    await queuedJob3.sync();
    t.equal(queuedJob3.state, JobState.Succeeded);

    await worker.unregister();
  });

  await t.test('Retry and remove job', async (t) => {
    const queuedJob1 = await queue.addJob('add', { first: 5, second: 6 });
    const worker = await queue.getNewWorker().register();
    const executor1 = (await worker.getNextExecutor())!;
    t.equal(executor1.id, queuedJob1.id);
    await queuedJob1.sync();
    t.equal(queuedJob1.maxAttempts, 1);
    t.equal(executor1.job.attempt, 1);
    t.ok(await executor1.markSucceeded());

    t.ok(await queuedJob1.sync());
    t.notOk(queuedJob1.retriedAt);
    const queuedJob1a = (await queuedJob1.retry())!;
    t.equal(queuedJob1a.state, JobState.Pending);
    t.equal(queuedJob1a.maxAttempts, 2);
    t.equal(queuedJob1a.attempt, 2);
    t.same(queuedJob1a.retriedAt instanceof Date, true);

    const executor3 = (await worker.getNextExecutor())!;
    await queuedJob1a.sync();
    t.equal(executor3.id, queuedJob1.id);
    t.equal(queuedJob1a.maxAttempts, 2);
    t.equal(executor3.job.attempt, 2);
    const queuedJob1b = (await queuedJob1a.retry())!;
    t.equal(queuedJob1b.maxAttempts, 3);
    t.equal(queuedJob1b.attempt, 3);

    const job4 = (await worker.getNextExecutor())!;
    t.equal(job4.id, queuedJob1.id);
    t.equal(job4.state, JobState.Running);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Running);
    t.ok(await job4.markSucceeded());
    t.ok(await queuedJob1.remove());
    t.notOk(await queuedJob1.retry());
    t.notOk(await job4.markSucceeded());

    const queuedJob2 = await queue.addJob('add', { first: 6, second: 5 });
    t.equal(queuedJob2.state, JobState.Pending);
    t.equal(queuedJob2.maxAttempts, 1);
    t.equal(queuedJob2.attempt, 1);
    t.ok(await queuedJob2.retry());
    await queuedJob2.sync();
    t.equal(queuedJob2.state, JobState.Pending);
    t.equal(queuedJob2.maxAttempts, 2);
    t.equal(queuedJob2.attempt, 2);

    const job6 = (await worker.getNextExecutor())!;
    t.equal(job6.id, queuedJob2.id);
    t.ok(await job6.markFailed({ oopsie: 'Fail and remove immediately' }));
    t.ok(await queuedJob2.remove());
    t.notOk(await job6.markFailed());

    const queuedJob3 = await queue.addJob('add', { first: 5, second: 5 });
    await queue.getJob(queuedJob3.id);
    t.ok(await queuedJob3.remove());

    await worker.unregister();
  });

  await t.test('Jobs with priority', async (t) => {
    await queue.addJob('add', { first: 1, second: 2 });
    const queuedJob1 = await queue.addJob('add', { first: 2, second: 4 }, { priority: 1 });
    const worker = await queue.getNewWorker().register();
    const job1 = (await worker.getNextExecutor())!;
    t.equal(job1.id, queuedJob1.id);
    await queuedJob1.sync();
    t.equal(queuedJob1.priority, 1);
    t.equal(queuedJob1.maxAttempts, 1);
    t.equal(queuedJob1.attempt, 1);
    t.ok(await job1.markSucceeded());
    t.not((await worker.getNextExecutor())!.id, queuedJob1.id);
    const queuedJob2 = await queue.addJob('add', { first: 2, second: 5 });
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, queuedJob2.id);
    t.equal(queuedJob2.priority, 0);
    t.ok(await job2.markSucceeded());
    const queuedJob2a = (await queuedJob2.retry({ priority: 100 }))!;
    const job3 = (await worker.getNextExecutor())!;
    t.equal(job3.id, queuedJob2a.id);
    t.equal(queuedJob2a.priority, 100);
    t.equal(queuedJob2a.maxAttempts, 2);
    t.equal(queuedJob2a.attempt, 2);
    t.ok(await job3.markSucceeded());
    const queuedJob2b = (await queuedJob2a.retry({ priority: 0 }))!;
    const job4 = (await worker.getNextExecutor())!;
    t.equal(job4.id, queuedJob2b.id);
    t.equal(queuedJob2b.priority, 0);
    t.equal(queuedJob2b.maxAttempts, 3);
    t.equal(queuedJob2b.attempt, 3);
    t.ok(await job4.markSucceeded());

    const queuedJob3 = await queue.addJob('add', { first: 2, second: 6 }, { priority: 2 });
    t.notOk(await worker.getNextExecutor(0, { minPriority: 5 }));
    t.notOk(await worker.getNextExecutor(0, { minPriority: 3 }));
    const job5 = (await worker.getNextExecutor(0, { minPriority: 2 }))!;
    t.equal(job5.id, queuedJob3.id);
    await queuedJob3.sync();
    t.equal(queuedJob3.priority, 2);
    t.ok(await job5.markSucceeded());
    const queuedJob4 = await queue.addJob('add', { first: 2, second: 8 }, { priority: 0 });
    const queuedJob5 = await queue.addJob('add', { first: 2, second: 7 }, { priority: 5 });
    const queuedJob6 = await queue.addJob('add', { first: 2, second: 8 }, { priority: -2 });
    t.notOk(await worker.getNextExecutor(0, { minPriority: 6 }));
    const job6 = (await worker.getNextExecutor(0, { minPriority: 0 }))!;
    t.equal(job6.id, queuedJob5.id);
    t.ok(await job6.markSucceeded());
    const job7 = (await worker.getNextExecutor(0, { minPriority: 0 }))!;
    t.equal(job7.id, queuedJob4.id);
    t.ok(await job7.markSucceeded());
    t.notOk(await worker.getNextExecutor(0, { minPriority: 0 }));
    const job8 = (await worker.getNextExecutor(0, { minPriority: -10 }))!;
    t.equal(job8.id, queuedJob6.id);
    t.ok(await job8.markSucceeded());
    await worker.unregister();
  });

  await t.test('Delayed jobs', async (t) => {
    const queuedJob1 = await queue.addJob('add', { first: 2, second: 1 }, { delayFor: 100000 });

    t.equal((await queue.getStatistics()).scheduledJobs, 1);
    const worker = await queue.getNewWorker().register();
    t.notOk(await worker.getNextExecutor());
    t.ok(await queuedJob1.sync());
    t.ok(queuedJob1.delayUntil > queuedJob1.createdAt);
    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() - INTERVAL '1 day' WHERE id = $1`, [queuedJob1.id]);
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, queuedJob1.id);
    t.ok(await job2.markSucceeded());
    t.ok(await queuedJob1.retry());
    await queuedJob1.sync();
    t.ok(queuedJob1.delayUntil <= queuedJob1.retriedAt!);
    t.ok(await queuedJob1.remove());
    t.notOk(await queuedJob1.retry());

    const queuedJob2 = await queue.addJob('add', { first: 6, second: 9 });
    const job4 = (await worker.getNextExecutor())!;
    t.equal(job4.id, queuedJob2.id);
    await queuedJob2.sync();
    t.ok(queuedJob2.delayUntil <= queuedJob2.createdAt);
    t.ok(await job4.markFailed());
    t.ok(await queuedJob2.retry({ delayFor: 100000 }));
    await queuedJob2.sync();
    t.equal(queuedJob2.maxAttempts, 2);
    t.equal(queuedJob2.attempt, 2);
    t.ok(queuedJob2.delayUntil > queuedJob2.retriedAt!);
    t.ok(await queue.getJob(queuedJob2.id).then((job) => job!.remove()));

    await worker.unregister();
  });

  await t.test('Queues', async (t) => {
    const queuedJob1 = await queue.addJob('add', { first: 100, second: 1 });
    const worker = await queue.getNewWorker().register();
    t.notOk(await worker.getNextExecutor(0, { queueNames: 'test1' }));
    const job1 = (await worker.getNextExecutor())!;
    t.equal(job1.id, queuedJob1.id);
    await queuedJob1.sync();
    t.equal(queuedJob1.queueName, 'default');
    t.ok(await job1.markSucceeded());

    const queuedJob2 = await queue.addJob('add', { first: 100, second: 3 }, { queueName: 'test1' });
    t.notOk(await worker.getNextExecutor());
    const job2 = (await worker.getNextExecutor(0, { queueNames: 'test1' }))!;
    t.equal(job2.id, queuedJob2.id);
    await queuedJob2.sync();
    t.equal(queuedJob2.queueName, 'test1');
    t.ok(await job2.markSucceeded());
    t.ok(await queuedJob2.retry({ queueName: 'test2' }));
    const job3 = (await worker.getNextExecutor(0, { queueNames: ['default', 'test2'] }))!;
    t.equal(job3.id, queuedJob2.id);
    await queuedJob2.sync();
    t.equal(queuedJob2.queueName, 'test2');
    t.ok(await job3.markSucceeded());
    await worker.unregister();
  });

  await t.test('Failed jobs', async (t) => {
    const queuedJob1 = await queue.addJob('add', { first: 5, second: 6 });
    const worker = await queue.getNewWorker().register();
    const job1 = (await worker.getNextExecutor())!;
    t.equal(job1.id, queuedJob1.id);
    t.equal(job1.progress, 0.0);
    t.equal(await job1.updateProgress(0.5), true);
    t.equal(job1.progress, 0.5);
    await queuedJob1.sync();
    t.notOk(queuedJob1.result);
    t.equal(queuedJob1.progress, 0.5);
    t.ok(await job1.markFailed());
    t.notOk(await job1.markSucceeded());
    await queuedJob1.sync();
    t.match(queuedJob1.result, {
      name: 'Error',
      message: 'Unknown error',
      stack: /at \w+\.markFailed/,
    });
    t.equal(queuedJob1.state, JobState.Failed);
    t.equal(queuedJob1.progress, 0.5);
    t.equal(job1.progress, 0.5);

    const queuedJob2 = await queue.addJob('add', { first: 6, second: 7 });
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, queuedJob2.id);
    t.ok(await job2.markFailed({ oops: 'Something bad happened' }));
    await queuedJob2.sync();
    t.equal(queuedJob2.state, JobState.Failed);
    t.same(queuedJob2.result, { oops: 'Something bad happened' });

    const queuedJob3 = await queue.addJob('fail');
    const job3 = (await worker.getNextExecutor())!;
    t.equal(job3.id, queuedJob3.id);
    await job3.perform(worker);
    await queuedJob3.sync();
    t.equal(queuedJob3.state, JobState.Failed);
    t.match(queuedJob3.result, {
      name: 'Error',
      message: /Intentional failure/,
      stack: /Intentional failure/,
    });
    await worker.unregister();
  });

  await t.test('Nested data structures', async (t) => {
    queue.registerTask('nested', async (job) => {
      const { object, array } = job.args as any;
      await job.amendMetadata({ bar: { baz: [1, 2, 3] } });
      await job.amendMetadata({ baz: 'yada' });
      return [{ 23: object.first[0].second + array[0][0] }];
    });
    const queuedJob1 = await queue.addJob(
      'nested',
      { object: { first: [{ second: 'test' }] }, array: [[3]] },
      { metadata: { foo: [4, 5, 6] } },
    );
    const worker = await queue.getNewWorker().register();
    const job = (await worker.getNextExecutor())!;
    await job.perform(worker);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Succeeded);

    t.ok(await job.amendMetadata({ yada: ['works'] }));
    await queuedJob1.sync();
    t.same(queuedJob1.metadata, { foo: [4, 5, 6], bar: { baz: [1, 2, 3] }, baz: 'yada', yada: ['works'] });
    t.same(queuedJob1.result, [{ 23: 'test3' }]);

    t.ok(await job.amendMetadata({ yada: null, bar: null }));
    await queuedJob1.sync();
    t.same(queuedJob1.metadata, { foo: [4, 5, 6], baz: 'yada' });

    t.notOk(await backend.amendJobMetadata(-1, { yada: [JobState.Failed] }));

    await worker.unregister();
  });

  await t.test('Multiple attempts with backoff while processing', async (t) => {
    const queuedJob1 = await queue.addJob('fail', {}, { maxAttempts: 3 });
    const worker = await queue.getNewWorker().register();
    const executor1 = (await worker.getNextExecutor())!;
    const job1 = executor1.job;
    t.equal(executor1.id, queuedJob1.id);
    t.equal(job1.attempt, 1);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Running);
    t.equal(queuedJob1.maxAttempts, 3);
    t.equal(queuedJob1.attempt, 1);
    await executor1.perform(worker);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Scheduled);
    t.match(queuedJob1.result, { message: /Intentional failure/ });
    t.equal(queuedJob1.maxAttempts, 3);
    t.equal(queuedJob1.attempt, 2);
    t.ok(queuedJob1.retriedAt! < queuedJob1.delayUntil);

    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() WHERE id = $1`, [queuedJob1.id]); // Skip backoff

    const executor2 = (await worker.getNextExecutor())!;
    const job2 = executor2.job;
    t.equal(executor2.id, queuedJob1.id);
    t.equal(job2.attempt, 2);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Running);
    t.equal(queuedJob1.maxAttempts, 3);
    t.equal(queuedJob1.attempt, 2);
    await executor2.perform(worker);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Scheduled);
    t.equal(queuedJob1.maxAttempts, 3);
    t.equal(queuedJob1.attempt, 3);

    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() WHERE id = $1`, [queuedJob1.id]); // Skip backoff again

    const executor3 = (await worker.getNextExecutor())!;
    const job3 = executor3.job;
    t.equal(executor3.id, queuedJob1.id);
    t.equal(job3.attempt, 3);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Running);
    t.equal(queuedJob1.maxAttempts, 3);
    t.equal(queuedJob1.attempt, 3);
    await executor3.perform(worker);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Failed);
    t.match(queuedJob1.result, { message: /Intentional failure/ });
    t.equal(queuedJob1.maxAttempts, 3);
    t.equal(queuedJob1.attempt, 3);

    t.ok(await queuedJob1.sync());
    t.ok(await queuedJob1.retry({ maxAttempts: 5 }));
    const job4 = (await worker.getNextExecutor())!;
    t.equal(job4.id, queuedJob1.id);
    await job4.perform(worker);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Scheduled);

    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() WHERE id = $1`, [queuedJob1.id]); // Skip backoff

    const job5 = (await worker.getNextExecutor())!;
    t.equal(job5.id, queuedJob1.id);
    await job5.perform(worker);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Failed);
    await worker.unregister();
  });

  await t.test('Multiple attempts with backoff during maintenance', async (t) => {
    const queuedJob1 = await queue.addJob('fail', {}, { maxAttempts: 2 });
    const worker = await queue.getNewWorker().register();
    const executor1 = (await worker.getNextExecutor())!;
    t.equal(executor1.id, queuedJob1.id);
    t.equal(executor1.job.attempt, 1);
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Running);
    t.equal(queuedJob1.maxAttempts, 2);
    t.equal(queuedJob1.attempt, 1);
    await worker.unregister();

    await queue.prune();
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Scheduled);
    t.same(queuedJob1.result, { name: 'WorkerGoneError', message: 'Worker went away' });
    t.equal(queuedJob1.maxAttempts, 2);
    t.equal(queuedJob1.attempt, 2);
    t.ok(queuedJob1.retriedAt! < queuedJob1.delayUntil);

    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() WHERE id = $1`, [queuedJob1.id]); // Skip backoff

    const worker2 = await queue.getNewWorker().register();
    const executor2 = (await worker2.getNextExecutor())!;
    t.equal(executor2.id, queuedJob1.id);
    t.equal(executor2.job.attempt, 2);
    await worker2.unregister();
    await queue.prune();
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Abandoned);
    t.same(queuedJob1.result, { name: 'WorkerGoneError', message: 'Worker went away' });
  });

  await t.test('A job needs to be dequeued again after a retry', async (t) => {
    queue.registerTask('restart', async () => {
      return;
    });
    const queuedJob1 = await queue.addJob('restart');
    const worker = await queue.getNewWorker().register();
    const executor1 = (await worker.getNextExecutor())!;
    t.equal(executor1.id, queuedJob1.id);
    t.ok(await executor1.markSucceeded());
    await queuedJob1.sync();
    t.equal(queuedJob1.state, JobState.Succeeded);
    const queuedJob2 = (await queuedJob1.retry())!;
    t.equal(queuedJob2.state, JobState.Pending);

    const executor2 = (await worker.getNextExecutor())!;
    t.equal(executor2.id, queuedJob1.id);
    await queuedJob2.sync();
    t.equal(queuedJob2.state, JobState.Running);
    t.notOk(await executor1.markSucceeded());
    await queuedJob2.sync();
    t.equal(queuedJob2.state, JobState.Running);
    t.ok(await executor2.markSucceeded());
    t.notOk(await queuedJob1.retry());
    await queuedJob2.sync();
    t.equal(queuedJob2.state, JobState.Succeeded);
    await worker.unregister();
  });

  await t.test('Perform jobs concurrently', async (t) => {
    const queuedJob1 = await queue.addJob('add', { first: 10, second: 11 });
    const queuedJob2 = await queue.addJob('add', { first: 12, second: 13 });
    const queuedJob3 = await queue.addJob('test');
    const queuedJob4 = await queue.addJob('fail');
    const worker = await queue.getNewWorker().register();
    const job1 = (await worker.getNextExecutor())!;
    const job2 = (await worker.getNextExecutor())!;
    const job3 = (await worker.getNextExecutor())!;
    const job4 = (await worker.getNextExecutor())!;
    await Promise.all([job1.perform(worker), job2.perform(worker), job3.perform(worker), job4.perform(worker)]);
    await Promise.all([queuedJob1.sync(), queuedJob2.sync(), queuedJob3.sync(), queuedJob4.sync()]);
    t.equal(queuedJob1.state, JobState.Succeeded);
    t.equal(queuedJob2.state, JobState.Succeeded);
    t.equal(queuedJob3.state, JobState.Succeeded);
    t.equal(queuedJob4.state, JobState.Failed);
    await worker.unregister();
  });

  await t.test('Job dependencies', async (t) => {
    await queue.prune({ jobExpungePeriod: 0 });
    t.equal((await queue.getStatistics()).succeededJobs, 0);
    const worker = await queue.getNewWorker().register();

    const queuedJob1 = await queue.addJob('test');
    const queuedJob2 = await queue.addJob('test');
    const queuedJob3 = await queue.addJob('test', {}, { parentJobIds: [queuedJob1.id, queuedJob2.id] });
    const job1 = (await worker.getNextExecutor())!;
    t.equal(job1.id, queuedJob1.id);
    await queuedJob1.sync();
    t.same(await queuedJob1.getChildJobIds(), [queuedJob3.id]);
    t.same(queuedJob1.parentJobIds, []);
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, queuedJob2.id);
    await queuedJob2.sync();
    t.same(await queuedJob2.getChildJobIds(), [queuedJob3.id]);
    t.same(queuedJob2.parentJobIds, []);
    t.notOk(await worker.getNextExecutor());
    t.ok(await job1.markSucceeded());
    t.notOk(await worker.getNextExecutor());
    t.ok(await job2.markFailed());
    t.notOk(await worker.getNextExecutor());
    t.ok(await queuedJob2.retry());
    const job3 = (await worker.getNextExecutor())!;
    t.equal(job3.id, queuedJob2.id);
    t.ok(await job3.markSucceeded());
    const job4 = (await worker.getNextExecutor())!;
    t.equal(job4.id, queuedJob3.id);
    await queuedJob3.sync();
    t.same(await queuedJob3.getChildJobIds(), []);
    t.same(queuedJob3.parentJobIds, [queuedJob1.id, queuedJob2.id]);

    t.equal((await queue.getStatistics()).succeededJobs, 2);
    await queue.prune({ jobExpungePeriod: 0 });
    t.equal((await queue.getStatistics()).succeededJobs, 0);
    t.ok(await job4.markSucceeded());
    t.equal((await queue.getStatistics()).succeededJobs, 1);
    await queue.prune({ jobExpungePeriod: 0 });
    t.equal((await queue.getStatistics()).succeededJobs, 0);

    const queuedJob4 = await queue.addJob('test', {}, { parentJobIds: [-1] });
    const job5 = (await worker.getNextExecutor())!;
    t.equal(job5.id, queuedJob4.id);
    t.ok(await job5.markSucceeded());
    const queuedJob5 = await queue.addJob('test', {}, { parentJobIds: [-1] });
    const job6 = (await worker.getNextExecutor())!;
    t.equal(job6.id, queuedJob5.id);
    await queuedJob5.sync();
    t.same(queuedJob5.parentJobIds, [-1]);
    const queuedJob5a = (await queuedJob5.retry({ parentJobIds: [-1, -2] }))!;
    const job7 = (await worker.getNextExecutor())!;
    await queuedJob5a.sync();
    t.same(queuedJob5a.parentJobIds, [-1, -2]);
    t.ok(await job7.markSucceeded());

    const queuedJob6 = await queue.addJob('test');
    const queuedJob7 = await queue.addJob('test');
    const queuedJob8 = await queue.addJob('test', {}, { parentJobIds: [queuedJob6.id, queuedJob7.id] });
    const parentIds = queuedJob8.parentJobIds;
    t.equal(parentIds.length, 2);
    t.equal(parentIds[0], queuedJob6.id);
    t.equal(parentIds[1], queuedJob7.id);
    t.ok(await queuedJob6.remove());
    t.ok(await queuedJob7.remove());
    t.ok(await queuedJob8.remove());

    await worker.unregister();
  });

  await t.test('Job dependencies (lax)', async (t) => {
    const worker = await queue.getNewWorker().register();
    const queuedJob1 = await queue.addJob('test');
    const queuedJob2 = await queue.addJob('test');
    const queuedJob3 = await queue.addJob(
      'test',
      {},
      { laxDependency: true, parentJobIds: [queuedJob1.id, queuedJob2.id] },
    );
    const job1 = (await worker.getNextExecutor())!;
    t.equal(job1.id, queuedJob1.id);
    await queuedJob1.sync();
    t.same(await queuedJob1.getChildJobIds(), [queuedJob3.id]);
    t.same(queuedJob1.parentJobIds, []);
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, queuedJob2.id);
    await queuedJob2.sync();
    t.same(await queuedJob2.getChildJobIds(), [queuedJob3.id]);
    t.same(queuedJob2.parentJobIds, []);
    t.notOk(await worker.getNextExecutor());
    t.ok(await job1.markSucceeded());
    t.notOk(await worker.getNextExecutor());
    t.ok(await job2.markFailed());
    const job3 = (await worker.getNextExecutor())!;
    t.equal(job3.id, queuedJob3.id);
    await queuedJob3.sync();
    t.same(await queuedJob3.getChildJobIds(), []);
    t.same(queuedJob3.parentJobIds, [queuedJob1.id, queuedJob2.id]);
    t.ok(await job3.markSucceeded());

    const queuedJob4 = await queue.addJob('test');
    const queuedJob5 = await queue.addJob('test', {}, { parentJobIds: [queuedJob4.id] });
    const job4 = (await worker.getNextExecutor())!;
    t.equal(job4.id, queuedJob4.id);
    t.notOk(await worker.getNextExecutor());
    t.ok(await job4.markFailed());
    t.notOk(await worker.getNextExecutor());
    t.ok(await queue.getJob(queuedJob5.id).then((job) => job!.retry({ laxDependency: true })));
    const job5 = (await worker.getNextExecutor())!;
    t.equal(job5.id, queuedJob5.id);
    await queuedJob5.sync();
    t.same(await queuedJob5.getChildJobIds(), []);
    t.same(queuedJob5.parentJobIds, [queuedJob4.id]);
    t.ok(await job5.markSucceeded());
    t.ok(await queuedJob4.remove());

    t.same((await queue.listJobInfos({ ids: [queuedJob5.id] }).next())!.laxDependency, true);
    t.ok(await queue.getJob(queuedJob5.id).then((job) => job!.retry()));
    t.same((await queue.listJobInfos({ ids: [queuedJob5.id] }).next())!.laxDependency, true);
    t.ok(await queue.getJob(queuedJob5.id).then((job) => job!.retry({ laxDependency: false })));
    t.same((await queue.listJobInfos({ ids: [queuedJob5.id] }).next())!.laxDependency, false);
    t.ok(await queue.getJob(queuedJob5.id).then((job) => job!.retry()));
    t.same((await queue.listJobInfos({ ids: [queuedJob5.id] }).next())!.laxDependency, false);
    t.ok(await queue.getJob(queuedJob5.id).then((job) => job!.remove()));
    await worker.unregister();
  });

  await t.test('Expiring jobs', async (t) => {
    const queuedJob1 = await queue.addJob('test');
    t.notOk(queuedJob1.expiresAt);
    t.ok(await queue.getJob(queuedJob1.id).then((job) => job!.remove()));

    const queuedJob2 = await queue.addJob('test', {}, { expireIn: 300000 });
    t.same(queuedJob2.expiresAt instanceof Date, true);
    const worker = await queue.getNewWorker().register();
    const job1 = (await worker.getNextExecutor())!;
    t.equal(job1.id, queuedJob2.id);
    await queuedJob2.sync();
    const expires = queuedJob2.expiresAt;
    t.same(expires instanceof Date, true);
    t.ok(await job1.markSucceeded());
    t.ok(await queuedJob2.retry({ expireIn: 600000 }));
    await queuedJob2.sync();
    t.equal(queuedJob2.state, JobState.Pending);
    t.same(queuedJob2.expiresAt instanceof Date, true);
    t.not(queuedJob2.expiresAt!.getTime(), expires!.getTime());
    await queue.prune();
    t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 1);
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, queuedJob2.id);
    t.ok(await job2.markSucceeded());

    const queuedJob3 = await queue.addJob('test', {}, { expireIn: 300000 });
    t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 1);
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [queuedJob3.id]);
    await queue.prune();
    t.notOk(await worker.getNextExecutor());
    t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 0);

    const queuedJob4 = await queue.addJob('test', {}, { expireIn: 300000 });
    const job4 = (await worker.getNextExecutor())!;
    t.equal(job4.id, queuedJob4.id);
    t.ok(await job4.markSucceeded());
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [queuedJob4.id]);
    await queue.prune();
    await queuedJob4.sync();
    t.equal(queuedJob4.state, JobState.Succeeded);

    const queuedJob5 = await queue.addJob('test', {}, { expireIn: 300000 });
    const job5 = (await worker.getNextExecutor())!;
    t.equal(job5.id, queuedJob5.id);
    t.ok(await job5.markFailed());
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [queuedJob5.id]);
    await queue.prune();
    await queuedJob5.sync();
    t.equal(queuedJob5.state, JobState.Failed);

    const queuedJob6 = await queue.addJob('test', {}, { expireIn: 300000 });
    const job6 = (await worker.getNextExecutor())!;
    t.equal(job6.id, queuedJob6.id);
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [queuedJob6.id]);
    await queue.prune();
    await queuedJob6.sync();
    t.equal(queuedJob6.state, JobState.Running);
    t.ok(await job6.markSucceeded());

    const queuedJob7 = await queue.addJob('test', {}, { expireIn: 300000 });
    const queuedJob8 = await queue.addJob('test', {}, { expireIn: 300000, parentJobIds: [queuedJob7.id] });
    t.notOk(await worker.getNextExecutor(0, { id: queuedJob8.id }));
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [queuedJob7.id]);
    await queue.prune();
    const job8 = (await worker.getNextExecutor(0, { id: queuedJob8.id }))!;
    t.ok(await job8.markSucceeded());
    await worker.unregister();
  });

  await t.test('runJobs', async (t) => {
    queue.registerTask('record_pid', async () => {
      return { pid: process.pid };
    });

    const queuedJob1 = await queue.addJob('record_pid');
    const queuedJob2 = await queue.addJob('fail');
    const queuedJob3 = await queue.addJob('record_pid');
    await queue.runJobs();
    await queuedJob1.sync();
    t.equal(queuedJob1.taskName, 'record_pid');
    t.equal(queuedJob1.state, JobState.Succeeded);
    t.same(queuedJob1.result, { pid: process.pid });
    await queuedJob2.sync();
    t.equal(queuedJob2.taskName, 'fail');
    t.equal(queuedJob2.state, JobState.Failed);
    t.match(queuedJob2.result, { message: /Intentional failure!/ });
    await queuedJob3.sync();
    t.equal(queuedJob3.taskName, 'record_pid');
    t.equal(queuedJob3.state, JobState.Succeeded);
    t.same(queuedJob3.result, { pid: process.pid });

    const queuedJob4 = await queue.addJob('record_pid');
    await queue.runJobs();
    await queuedJob4.sync();
    t.equal(queuedJob4.taskName, 'record_pid');
    t.equal(queuedJob4.state, JobState.Succeeded);
    t.same(queuedJob4.result, { pid: process.pid });
  });

  await t.test('runJob', async (t) => {
    const queuedJob1 = await queue.addJob('test', {}, { maxAttempts: 2 });
    const queuedJob2 = await queue.addJob('test');
    const queuedJob3 = await queue.addJob('test', {}, { parentJobIds: [queuedJob1.id, queuedJob2.id] });
    t.notOk(await queue.runJob(queuedJob3.id));

    await queuedJob1.sync();
    t.equal(queuedJob1.queueName, 'default');
    t.equal(queuedJob1.state, JobState.Pending);
    t.equal(queuedJob1.maxAttempts, 2);
    t.equal(queuedJob1.attempt, 1);
    t.ok(await queue.runJob(queuedJob1.id));

    await queuedJob1.sync();
    t.equal(queuedJob1.queueName, backend.FOREGROUND_QUEUE);
    t.equal(queuedJob1.state, JobState.Succeeded);
    t.equal(queuedJob1.maxAttempts, 3);
    t.equal(queuedJob1.attempt, 2);

    t.ok(await queue.runJob(queuedJob2.id));
    await queuedJob2.sync();
    t.equal(queuedJob2.queueName, backend.FOREGROUND_QUEUE);
    t.equal(queuedJob2.state, JobState.Succeeded);
    t.equal(queuedJob2.maxAttempts, 2);
    t.equal(queuedJob2.attempt, 2);

    t.ok(await queue.runJob(queuedJob3.id));
    await queuedJob3.sync();
    t.equal(queuedJob3.queueName, backend.FOREGROUND_QUEUE);
    t.equal(queuedJob3.state, JobState.Succeeded);
    t.equal(queuedJob3.maxAttempts, 3);
    t.equal(queuedJob3.attempt, 3);

    t.notOk(await queue.runJob(queuedJob3.id + 1));

    const queuedJob4 = await queue.addJob('fail');
    let result;
    try {
      await queue.runJob(queuedJob4.id);
    } catch (error) {
      result = error;
    }
    t.match(result, { message: /Intentional failure/ });
    await queuedJob4.sync();
    t.ok(queuedJob4.workerId);
    t.equal((await queue.getStatistics()).onlineWorkers, 0);
    t.equal(queuedJob4.maxAttempts, 2);
    t.equal(queuedJob4.attempt, 2);
    t.equal(queuedJob4.state, JobState.Failed);
    t.equal(queuedJob4.queueName, backend.FOREGROUND_QUEUE);
    t.match(queuedJob4.result, { message: /Intentional failure/ });
  });

  await t.test('Reset (all)', async (t) => {
    await queue.resetQueue();
    await queue.addJob('test');
    await queue.getNewWorker().register();
    t.equal(await queue.listJobInfos().numRows(), 1);
    t.equal(await queue.listWorkerInfos().numRows(), 1);

    await queue.resetQueue();
    t.equal(await queue.listJobInfos().numRows(), 0);
    t.equal(await queue.listWorkerInfos().numRows(), 0);
  });

  await t.test('Stats', async (t) => {
    await queue.resetQueue();

    const stats1 = await queue.getStatistics();
    t.equal(stats1.enqueuedJobs, 0);
    t.equal(stats1.pendingJobs, 0);
    t.equal(stats1.scheduledJobs, 0);
    t.equal(stats1.runningJobs, 0);
    t.equal(stats1.succeededJobs, 0);
    t.equal(stats1.failedJobs, 0);
    t.equal(stats1.abandonedJobs, 0);
    t.equal(stats1.canceledJobs, 0);
    t.equal(stats1.onlineWorkers, 0);
    t.equal(stats1.busyWorkers, 0);
    t.equal(stats1.idleWorkers, 0);
    t.equal(stats1.queueboneVersion, '0.4.1');
    t.equal(stats1.backendName, 'Pg');
    t.match(stats1.backendVersion, /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/);
    t.ok(stats1.backendUptime);

    const worker = await queue.getNewWorker().register();
    t.equal((await queue.getStatistics()).onlineWorkers, 1);
    t.equal((await queue.getStatistics()).idleWorkers, 1);
    const queuedJob1 = await queue.addJob('fail');
    t.equal((await queue.getStatistics()).enqueuedJobs, 1);
    const queuedJob2 = await queue.addJob('fail');
    t.equal((await queue.getStatistics()).enqueuedJobs, 2);
    t.equal((await queue.getStatistics()).pendingJobs, 2);

    const job1 = (await worker.getNextExecutor(0))!;
    t.equal(job1.id, queuedJob1.id);
    const stats2 = await queue.getStatistics();
    t.equal(stats2.pendingJobs, 1);
    t.equal(stats2.runningJobs, 1);
    t.equal(stats2.onlineWorkers, 1);
    t.equal(stats2.busyWorkers, 1);

    const queuedJob3 = await queue.addJob('fail');
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, queuedJob2.id);
    const stats3 = await queue.getStatistics();
    t.equal(stats3.pendingJobs, 1);
    t.equal(stats3.runningJobs, 2);
    t.equal(stats3.busyWorkers, 1);

    t.same(await job2.markSucceeded(), true);
    t.same(await job1.markSucceeded(), true);
    t.equal((await queue.getStatistics()).succeededJobs, 2);
    const job3 = (await worker.getNextExecutor())!;
    t.equal(job3.id, queuedJob3.id);
    t.same(await job3.markFailed(), true);
    t.equal((await queue.getStatistics()).failedJobs, 1);
    t.ok(await queuedJob3.retry());
    t.equal((await queue.getStatistics()).failedJobs, 0);

    const job4 = (await worker.getNextExecutor())!;
    await job4.markSucceeded({ it: 'works' });
    await worker.unregister();
    const stats4 = await queue.getStatistics();
    t.equal(stats4.pendingJobs, 0);
    t.equal(stats4.runningJobs, 0);
    t.equal(stats4.succeededJobs, 3);
    t.equal(stats4.failedJobs, 0);
    t.equal(stats4.abandonedJobs, 0);
    t.equal(stats4.canceledJobs, 0);
    t.equal(stats4.offlineWorkers, 1);
    t.equal(stats4.onlineWorkers, 0);
    t.equal(stats4.busyWorkers, 0);
    t.equal(stats4.idleWorkers, 0);

    await worker.unregister();
  });

  await t.test('Job history', async (t) => {
    await queue.addJob('fail');
    const worker = await queue.getNewWorker().register();
    const job = (await worker.getNextExecutor())!;
    t.ok(await job.markFailed());
    await worker.unregister();
    const history = await queue.getJobStatistics();
    t.equal(history.daily.length, 24);
    t.equal(history.daily[23].succeededJobs + history.daily[22].succeededJobs, 3);
    t.equal(history.daily[23].failedJobs + history.daily[22].failedJobs, 1);
    t.equal(history.daily[0].succeededJobs, 0);
    t.equal(history.daily[0].failedJobs, 0);
    t.ok(history.daily[0].epoch);
    t.ok(history.daily[1].epoch);
    t.ok(history.daily[12].epoch);
    t.ok(history.daily[23].epoch);
  });

  await queue.stop();

  // Clean up once we are done
  await pool.query('DROP SCHEMA queue_test CASCADE');

  await pool.end();
});
