import t from 'tap';
import { JOB_TABLE, PgBackend, WORKER_TABLE } from './backends/pg/backend.js';
import { createPool } from './backends/pg/factory.js';
import { DefaultQueue, type DefaultQueueInterface } from './queue.js';
import { type Backend } from './types/backend.js';
import { type JobResult, JobState } from './types/job.js';
import { type Task } from './types/task.js';
import { WorkerState } from './types/worker.js';
import { DefaultWorker } from './worker.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};

t.test('Queue with PostgreSQL backend', skip, async (t) => {
  const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=queue_test`);

  // Isolate tests
  await pool.query('DROP SCHEMA IF EXISTS queue_test CASCADE');
  await pool.query('CREATE SCHEMA queue_test');

  const backend: Backend = new PgBackend(pool);
  const queue: DefaultQueueInterface = new DefaultQueue(backend);
  await queue.updateSchema();

  // Register at some simple tasks for further tests
  queue.registerTask(
    new (class implements Task {
      readonly name = 'test';
      async handle(): Promise<JobResult> {}
    })(),
  );
  queue.registerTask('fail', async () => {
    throw new Error('Intentional failure!');
  });
  queue.registerTask('add', async (job) => {
    const { first, second } = job.args;
    return { added: first + second };
  });

  await t.test('Nothing to prune', async (t) => {
    t.notOk(await queue.prune());
  });

  await t.test('Job results', async (t) => {
    const worker = queue.getNewWorker();
    await worker.register();

    const id1 = await queue.addJob('test');
    const promise1 = queue.getJobResult(id1, { interval: 0 });
    const job1 = (await queue.assignNextJob(worker, 0))!;
    t.equal(job1.id, id1);
    t.same(job1.progress, 0.0);
    t.same(await job1.amendMetadata({ foo: 'bar' }), true);
    t.same(await job1.markSucceeded({ just: 'works' }), true);
    t.same(job1.progress, 1.0);
    const info1 = (await promise1)!;
    t.same(info1.result, { just: 'works' });
    t.same(info1.progress, 1.0);
    t.same(info1.metadata, { foo: 'bar' });

    let failed;
    const id2 = await queue.addJob('test');
    t.not(id2, id1);
    const promise2 = queue.getJobResult(id2, { interval: 0 }).catch((reason) => (failed = reason));
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id2);
    t.not(job2.id, id1);
    t.same(await job2.markFailed({ just: 'works too' }), true);
    await promise2;
    t.same(failed!.result, { just: 'works too' });

    const promise3 = queue.getJobResult(id1, { interval: 0 });
    const info2 = (await promise3)!;
    t.same(info2.result, { just: 'works' });
    t.same(info2.metadata, { foo: 'bar' });

    let succeeded;
    failed = undefined;
    const job3 = (await queue.getJob(id1))!;
    t.same(await job3.retry(), true);
    t.equal((await job3.getInfo())!.state, JobState.Pending);
    const ac = new AbortController();
    const signal = ac.signal;
    const promise4 = queue
      .getJobResult(id1, { interval: 10, signal })
      .then((value) => (succeeded = value))
      .catch((reason) => (failed = reason));
    setTimeout(() => ac.abort(), 250);
    await promise4;
    t.same(succeeded, undefined);
    t.same(failed!.name, 'AbortError');

    succeeded = undefined;
    failed = undefined;
    const job4 = (await queue.getJob(id1))!;
    t.same(await job4.remove(), true);
    const promise5 = queue
      .getJobResult(id1, { interval: 10, signal })
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
    const job = (await queue.assignNextJob(worker, 10000))!;
    t.notSame(job, null);
    await job.markSucceeded({ one: ['two', ['three']] });
    t.same((await job.getInfo())!.result, { one: ['two', ['three']] });
    await worker.unregister();
  });

  await t.test('Repair missing worker', async (t) => {
    const worker1 = await queue.getNewWorker().register();
    const worker2 = await queue.getNewWorker().register();
    t.not(worker1.id, worker2.id);

    const id = await queue.addJob('test');
    const job = (await queue.assignNextJob(worker2))!;
    t.equal(job.id, id);
    t.equal((await job.getInfo())!.state, JobState.Running);
    const workerId = worker2.id;
    const missingAfter = DefaultQueue.DEFAULT_OPTIONS.workerMissingTimeout + 1;
    t.ok(await worker2.getInfo());

    await pool.query(`UPDATE ${WORKER_TABLE} SET last_seen_at = NOW() - $1 * INTERVAL '1 millisecond' WHERE id = $2`, [
      missingAfter,
      workerId,
    ]);

    await queue.prune();
    t.equal((await worker2.getInfo())!.state, WorkerState.Missing);
    const info = (await job.getInfo())!;
    t.equal(info.state, JobState.Abandoned);
    t.equal(info.result, 'Worker went away');
    t.equal((await queue.getStatistics()).abandonedJobs, 1);
    await worker1.unregister();
    // don't unregister worker2 here for a listWorker test
  });

  await t.test('Repair abandoned job', async (t) => {
    const worker = await queue.getNewWorker().register();
    await queue.addJob('test');
    const job = (await queue.assignNextJob(worker))!;
    await worker.unregister();
    await queue.prune();
    const info = (await job.getInfo())!;
    t.equal(info.state, JobState.Abandoned);
    t.equal(info.result, 'Worker went away');
    t.equal((await queue.getStatistics()).abandonedJobs, 2);
  });

  await t.test('Repair abandoned job in foreground queue (have to be handled manually)', async (t) => {
    const worker = await queue.getNewWorker().register();
    const id = await queue.addJob('test', {}, { queueName: DefaultWorker.FOREGROUND_QUEUE });
    const job = (await queue.assignNextJob(worker, 0, { queueNames: [DefaultWorker.FOREGROUND_QUEUE] }))!;
    t.equal(job.id, id);
    await worker.unregister();
    await queue.prune();
    const info = (await job.getInfo())!;
    t.equal(info.state, JobState.Running);
    t.equal(info.queueName, DefaultWorker.FOREGROUND_QUEUE);
    t.same(info.result, null);
  });

  await t.test('Repair old jobs', async (t) => {
    t.equal(DefaultQueue.DEFAULT_OPTIONS.jobRetentionPeriod, 172800000);

    const worker = await queue.getNewWorker().register();
    const id1 = await queue.addJob('test');
    const id2 = await queue.addJob('test');
    const id3 = await queue.addJob('test');

    await queue.assignNextJob(worker).then((job) => job!.perform(worker));
    await queue.assignNextJob(worker).then((job) => job!.perform(worker));
    await queue.assignNextJob(worker).then((job) => job!.perform(worker));

    const result1 = await pool.query(
      `SELECT EXTRACT(EPOCH FROM finished_at) AS finished_at FROM ${JOB_TABLE} WHERE id = $1`,
      [id2],
    );
    const finishedAt1 = result1.rows[0].finished_at;
    await pool.query(`UPDATE ${JOB_TABLE} SET finished_at = TO_TIMESTAMP($1) WHERE id = $2`, [
      finishedAt1 - (DefaultQueue.DEFAULT_OPTIONS.jobRetentionPeriod + 1),
      id2,
    ]);
    const result2 = await pool.query(
      `SELECT EXTRACT(EPOCH FROM finished_at) AS finished_at FROM ${JOB_TABLE} WHERE id = $1`,
      [id3],
    );
    const finishedAt2 = result2.rows[0].finished_at;
    await pool.query(`UPDATE ${JOB_TABLE} SET finished_at = TO_TIMESTAMP($1) WHERE id = $2`, [
      finishedAt2 - (DefaultQueue.DEFAULT_OPTIONS.jobRetentionPeriod + 1),
      id3,
    ]);

    await worker.unregister();
    await queue.prune();
    t.ok(await queue.getJob(id1));
    t.notOk(await queue.getJob(id2));
    t.notOk(await queue.getJob(id3));
  });

  await t.test('Repair stuck jobs', async (t) => {
    t.equal(DefaultQueue.DEFAULT_OPTIONS.jobStuckTimeout, 172800000);

    const worker = await queue.getNewWorker().register();
    const id1 = await queue.addJob('test', { delayFor: 1000 });
    const id2 = await queue.addJob('test', { delayFor: 1000 });
    const id3 = await queue.addJob('test', { delayFor: 1000 });
    const id4 = await queue.addJob('test', { delayFor: 1000 });

    const stuck = DefaultQueue.DEFAULT_OPTIONS.jobStuckTimeout + 1;
    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() - $1 * INTERVAL '1 second' WHERE id = $2`, [
      stuck,
      id1,
    ]);
    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() - $1 * INTERVAL '1 second' WHERE id = $2`, [
      stuck,
      id2,
    ]);
    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() - $1 * INTERVAL '1 second' WHERE id = $2`, [
      stuck,
      id3,
    ]);
    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() - $1 * INTERVAL '1 second' WHERE id = $2`, [
      stuck,
      id4,
    ]);

    const job1 = (await queue.assignNextJob(worker, 0, { id: id4 }))!;
    await job1.markSucceeded('Works!');
    const job2 = (await queue.assignNextJob(worker, 0, { id: id2 }))!;
    await queue.prune();

    t.equal((await job2.getInfo())!.state, JobState.Running);
    t.ok(await job2.markSucceeded());

    t.equal((await queue.getStatistics()).stuckJobs, 2);
    const job3 = (await queue.getJob(id1))!;
    const info1 = (await job3.getInfo())!;
    t.equal(info1.state, JobState.Stuck);
    t.equal(info1.result, 'Job appears stuck in queue');
    const job4 = (await queue.getJob(id3))!;
    const info2 = (await job4.getInfo())!;
    t.equal(info2.state, JobState.Stuck);
    t.equal(info2.result, 'Job appears stuck in queue');

    const job5 = (await queue.getJob(id4))!;
    const info3 = (await job5.getInfo())!;
    t.equal(info3.state, JobState.Succeeded);
    t.equal(info3.result, 'Works!');

    await worker.unregister();
  });

  await t.test('Dealing with Worker metadata, and BackendIterator adapting to conditions', async (t) => {
    await queue.resetQueue();

    const worker1 = await queue.getNewWorker({ metadata: { test: 'one' } }).register();
    const worker2 = await queue.getNewWorker({ metadata: { test: 'two' } }).register();
    const worker3 = await queue.getNewWorker({ metadata: { test: 'three' } }).register();
    const worker4 = await queue.getNewWorker({ metadata: { test: 'four' } }).register();
    const worker5 = await queue.getNewWorker({ metadata: { test: 'five' } }).register();
    const workers = queue.listWorkerInfos({}, 2);
    t.notOk(workers.highestId);
    t.equal((await workers.next())!.metadata.test, 'one');
    t.equal(workers.highestId, 2);
    t.equal((await workers.next())!.metadata.test, 'two');
    t.equal((await workers.next())!.metadata.test, 'three');
    t.equal(workers.highestId, 4);
    t.equal((await workers.next())!.metadata.test, 'four');
    t.equal((await workers.next())!.metadata.test, 'five');
    t.equal(workers.highestId, 5);

    t.notOk(await workers.next());

    const workers1 = queue.listWorkerInfos({ ids: [2, 4, 1] });
    const result1: string[] = [];
    for await (const worker of workers1) {
      result1.push(worker.metadata.test);
    }
    t.same(result1, ['one', 'two', 'four']);

    const workers2 = queue.listWorkerInfos({ ids: [2, 4, 1] });
    // workers2.fetch is default
    t.notOk(workers2.highestId);
    t.equal((await workers2.next())!.metadata.test, 'one');
    t.equal(workers2.highestId, 4);
    t.equal((await workers2.next())!.metadata.test, 'two');
    t.equal((await workers2.next())!.metadata.test, 'four');
    t.notOk(await workers2.next());

    const workers3 = queue.listWorkerInfos({}, 2);
    t.equal((await workers3.next())!.metadata.test, 'one');
    t.equal((await workers3.next())!.metadata.test, 'two');
    t.equal(await workers3.numRows(), 5);
    await worker1.unregister();
    await worker2.unregister();
    await worker3.unregister();
    t.equal((await workers3.next())!.metadata.test, 'four');
    t.equal((await workers3.next())!.metadata.test, 'five');
    t.notOk(await workers3.next());
    t.equal(await workers3.numRows(), 4);
    t.equal(await queue.listWorkerInfos({}).numRows(), 2);
    await worker4.unregister();
    await worker5.unregister();
  });

  await t.test('Reset (all)', async (t) => {
    await queue.addJob('test');
    await queue.getNewWorker().register();
    t.equal(await queue.listJobInfos().numRows(), 1);
    t.equal(await queue.listWorkerInfos().numRows(), 1);

    await queue.resetQueue();

    t.equal(await queue.listJobInfos().numRows(), 0);
    t.equal(await queue.listWorkerInfos().numRows(), 0);
  });

  await t.test('Stats', async (t) => {
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
    await queue.addJob('fail');
    t.equal((await queue.getStatistics()).enqueuedJobs, 1);
    await queue.addJob('fail');
    t.equal((await queue.getStatistics()).enqueuedJobs, 2);
    t.equal((await queue.getStatistics()).pendingJobs, 2);

    const job1 = (await queue.assignNextJob(worker, 0))!;
    const stats2 = await queue.getStatistics();
    t.equal(stats2.pendingJobs, 1);
    t.equal(stats2.runningJobs, 1);
    t.equal(stats2.onlineWorkers, 1);
    t.equal(stats2.busyWorkers, 1);

    await queue.addJob('fail');
    const job2 = (await queue.assignNextJob(worker))!;
    const stats3 = await queue.getStatistics();
    t.equal(stats3.pendingJobs, 1);
    t.equal(stats3.runningJobs, 2);
    t.equal(stats3.busyWorkers, 1);

    t.same(await job2.markSucceeded(), true);
    t.same(await job1.markSucceeded(), true);
    t.equal((await queue.getStatistics()).succeededJobs, 2);
    const job3 = (await queue.assignNextJob(worker))!;
    t.same(await job3.markFailed(), true);
    t.equal((await queue.getStatistics()).failedJobs, 1);
    t.same(await job3.retry(), true);
    t.equal((await queue.getStatistics()).failedJobs, 0);

    const job4 = (await queue.assignNextJob(worker))!;
    await job4.markSucceeded(['works']);
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

  await t.test('History', async (t) => {
    await queue.addJob('fail');
    const worker = await queue.getNewWorker().register();
    const job = (await queue.assignNextJob(worker))!;
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

  await t.test('List jobs', async (t) => {
    const id1 = await queue.addJob('add');

    const jobs1 = queue.listJobInfos();
    t.equal((await jobs1.next())!.taskName, 'fail');
    t.equal(jobs1.highestId, 5);
    t.equal((await jobs1.next())!.taskName, 'fail');
    t.equal((await jobs1.next())!.taskName, 'fail');
    t.equal((await jobs1.next())!.taskName, 'fail');
    t.equal((await jobs1.next())!.taskName, 'add');
    t.notOk(await jobs1.next());
    t.equal(await jobs1.numRows(), 5);

    const jobs2 = queue.listJobInfos({ states: [JobState.Pending] });
    t.equal(await jobs2.numRows(), 1);
    t.equal((await jobs2.next())!.taskName, 'add');
    t.notOk(await jobs2.next());

    const jobs3 = queue.listJobInfos({ states: [JobState.Running] });
    t.notOk(await jobs3.next());

    const jobs4 = queue.listJobInfos({}, 2);
    t.notOk(jobs4.highestId);
    t.equal((await jobs4.next())!.taskName, 'fail');
    t.equal(jobs4.highestId, 2);
    t.equal((await jobs4.next())!.taskName, 'fail');
    t.equal(jobs4.highestId, 2);
    t.equal((await jobs4.next())!.taskName, 'fail');
    t.equal(jobs4.highestId, 4);
    t.equal((await jobs4.next())!.taskName, 'fail');
    t.equal(jobs4.highestId, 4);
    t.equal((await jobs4.next())!.taskName, 'add');
    t.equal(jobs4.highestId, 5);
    t.notOk(await jobs4.next());
    t.equal(await jobs4.numRows(), 5);

    await queue.getJob(id1).then((job) => job!.remove());
  });

  await t.test('Enqueue, dequeue and perform', async (t) => {
    t.notOk(await queue.getJob(12345));
    const id = await queue.addJob('add', { first: 2, second: 2 });
    const info1 = (await queue.getJob(id).then((job) => job!.getInfo()))!;
    t.same(info1.args, { first: 2, second: 2 });
    t.equal(info1.state, JobState.Pending);
    t.equal(info1.priority, 0);

    const worker = queue.getNewWorker();
    t.same(await queue.assignNextJob(worker), null);
    await worker.register();
    const job1 = (await queue.assignNextJob(worker))!;
    t.same((await worker.getInfo())!.jobs, [id]);
    t.equal(job1.taskName, 'add');
    t.equal(job1.attempt, 1);
    t.same(job1.args, { first: 2, second: 2 });
    const info2 = (await job1.getInfo())!;
    t.equal(info2.state, JobState.Running);
    t.equal(info2.workerId, worker.id);
    t.same(info2.createdAt instanceof Date, true);
    t.same(info2.startedAt instanceof Date, true);
    t.notOk(info2.finishedAt);
    t.same(info2.time instanceof Date, true);

    await job1.perform(worker);
    t.same((await worker.getInfo())!.jobs, []);
    const info3 = (await job1.getInfo())!;
    t.equal(info3.state, JobState.Succeeded);
    t.same(info3.result, { added: 4 });
    t.same(info3.finishedAt instanceof Date, true);
    await worker.unregister();

    const job2 = (await queue.getJob(job1.id))!;
    t.same(job2.taskName, 'add');
    t.same(job2.args, { first: 2, second: 2 });
    t.equal((await job2.getInfo())!.state, JobState.Succeeded);
  });

  await t.test('Cancel job', async (t) => {
    const worker = await queue.getNewWorker().register();

    const id1 = await queue.addJob('add', { first: 11, second: 17 }, { delayFor: 10000 });
    t.notOk(await queue.assignNextJob(worker));
    const job1 = (await queue.getJob(id1))!;
    t.equal((await job1.getInfo())!.state, JobState.Scheduled);
    t.ok(await job1.cancel());
    t.equal((await job1.getInfo())!.state, JobState.Canceled);

    const id2 = await queue.addJob('add', { first: 13, second: 29 });
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id2);
    t.equal((await job2.getInfo())!.state, JobState.Running);
    t.notOk(await job2.cancel());
    t.equal((await job2.getInfo())!.state, JobState.Running);

    const id3 = await queue.addJob('add', { first: 17, second: 29 });
    const job3 = (await queue.assignNextJob(worker))!;
    t.equal(job3.id, id3);
    await job3.markSucceeded();
    t.equal((await job3.getInfo())!.state, JobState.Succeeded);
    t.notOk(await job3.cancel());
    t.equal((await job3.getInfo())!.state, JobState.Succeeded);

    await worker.unregister();
  });

  await t.test('Retry and remove job', async (t) => {
    const id1 = await queue.addJob('add', { first: 5, second: 6 });
    const worker = await queue.getNewWorker().register();
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id1);
    const info1 = (await job1.getInfo())!;
    t.equal(info1.maxAttempts, 1);
    t.equal(info1.attempt, 1);
    t.ok(await job1.markSucceeded());

    const job2 = (await queue.getJob(id1))!;
    t.notOk((await job2.getInfo())!.retriedAt);
    t.ok(await job1.retry());
    const info2 = (await job2.getInfo())!;
    t.equal(info2.state, JobState.Pending);
    t.equal(info2.maxAttempts, 2);
    t.equal(info2.attempt, 2);
    t.same(info2.retriedAt instanceof Date, true);

    const job3 = (await queue.assignNextJob(worker))!;
    const info3 = (await job3.getInfo())!;
    t.equal(info3.maxAttempts, 2);
    t.equal(info3.attempt, 2);
    t.ok(await job3.retry());
    t.equal(job3.id, id1);
    const info4 = (await job3.getInfo())!;
    t.equal(info4.maxAttempts, 3);
    t.equal(info4.attempt, 3);

    const job4 = (await queue.assignNextJob(worker))!;
    t.equal((await job4.getInfo())!.state, JobState.Running);
    t.ok(await job4.markSucceeded());
    t.ok(await job4.remove());
    t.notOk(await job4.retry());
    t.notOk(await job4.getInfo());

    const id2 = await queue.addJob('add', { first: 6, second: 5 });
    const job5 = (await queue.getJob(id2))!;
    const info5 = (await job5.getInfo())!;
    t.equal(info5.state, JobState.Pending);
    t.equal(info5.maxAttempts, 1);
    t.equal(info5.attempt, 1);
    t.ok(await job5.retry());
    const info6 = (await job5.getInfo())!;
    t.equal(info6.state, JobState.Pending);
    t.equal(info6.maxAttempts, 2);
    t.equal(info6.attempt, 2);

    const job6 = (await queue.assignNextJob(worker))!;
    t.equal(job6.id, id2);
    t.ok(await job6.markFailed('Fail and remove immediately'));
    t.ok(await job6.remove());
    t.notOk(await job6.getInfo());

    const id3 = await queue.addJob('add', { first: 5, second: 5 });
    const job7 = (await queue.getJob(id3))!;
    t.ok(await job7.remove());

    await worker.unregister();
  });

  await t.test('Jobs with priority', async (t) => {
    await queue.addJob('add', { first: 1, second: 2 });
    const id1 = await queue.addJob('add', { first: 2, second: 4 }, { priority: 1 });
    const worker = await queue.getNewWorker().register();
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id1);
    const info1 = (await job1.getInfo())!;
    t.equal(info1.priority, 1);
    t.equal(info1.maxAttempts, 1);
    t.equal(info1.attempt, 1);
    t.ok(await job1.markSucceeded());
    t.not((await queue.assignNextJob(worker))!.id, id1);
    const id2 = await queue.addJob('add', { first: 2, second: 5 });
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id2);
    t.equal((await job2.getInfo())!.priority, 0);
    t.ok(await job2.markSucceeded());
    t.ok(await job2.retry({ priority: 100 }));
    const job3 = (await queue.assignNextJob(worker))!;
    t.equal(job3.id, id2);
    const info2 = (await job3.getInfo())!;
    t.equal(info2.priority, 100);
    t.equal(info2.maxAttempts, 2);
    t.equal(info2.attempt, 2);
    t.ok(await job3.markSucceeded());
    t.ok(await job3.retry({ priority: 0 }));
    const job4 = (await queue.assignNextJob(worker))!;
    t.equal(job4.id, id2);
    const info3 = (await job4.getInfo())!;
    t.equal(info3.priority, 0);
    t.equal(info3.maxAttempts, 3);
    t.equal(info3.attempt, 3);
    t.ok(await job4.markSucceeded());

    const id3 = await queue.addJob('add', { first: 2, second: 6 }, { priority: 2 });
    t.notOk(await queue.assignNextJob(worker, 0, { minPriority: 5 }));
    t.notOk(await queue.assignNextJob(worker, 0, { minPriority: 3 }));
    const job5 = (await queue.assignNextJob(worker, 0, { minPriority: 2 }))!;
    t.equal(job5.id, id3);
    t.equal((await job5.getInfo())!.priority, 2);
    t.ok(await job5.markSucceeded());
    await queue.addJob('add', { first: 2, second: 8 }, { priority: 0 });
    await queue.addJob('add', { first: 2, second: 7 }, { priority: 5 });
    await queue.addJob('add', { first: 2, second: 8 }, { priority: -2 });
    t.notOk(await queue.assignNextJob(worker, 0, { minPriority: 6 }));
    const job6 = (await queue.assignNextJob(worker, 0, { minPriority: 0 }))!;
    t.equal((await job6.getInfo())!.priority, 5);
    t.ok(await job6.markSucceeded());
    const job7 = (await queue.assignNextJob(worker, 0, { minPriority: 0 }))!;
    t.equal((await job7.getInfo())!.priority, 0);
    t.ok(await job7.markSucceeded());
    t.notOk(await queue.assignNextJob(worker, 0, { minPriority: 0 }));
    const job8 = (await queue.assignNextJob(worker, 0, { minPriority: -10 }))!;
    t.equal((await job8.getInfo())!.priority, -2);
    t.ok(await job8.markSucceeded());
    await worker.unregister();
  });

  await t.test('Delayed jobs', async (t) => {
    const id1 = await queue.addJob('add', { first: 2, second: 1 }, { delayFor: 100000 });
    t.equal((await queue.getStatistics()).scheduledJobs, 1);
    const worker = await queue.getNewWorker().register();
    t.notOk(await queue.assignNextJob(worker));
    const job1 = (await queue.getJob(id1))!;
    const info1 = (await job1.getInfo())!;
    t.ok(info1.delayUntil > info1.createdAt);
    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() - INTERVAL '1 day' WHERE id = $1`, [id1]);
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id1);
    t.same((await job2.getInfo())!.delayUntil instanceof Date, true);
    t.ok(await job2.markSucceeded());
    t.ok(await job2.retry());
    const job3 = (await queue.getJob(id1))!;
    const info2 = (await job3.getInfo())!;
    t.ok(info2.delayUntil <= info2.retriedAt);
    t.ok(await job3.remove());
    t.notOk(await job3.retry());

    const id2 = await queue.addJob('add', { first: 6, second: 9 });
    const job4 = (await queue.assignNextJob(worker))!;
    t.equal(job4.id, id2);
    const info3 = (await job4.getInfo())!;
    t.ok(info3.delayUntil <= info3.createdAt);
    t.ok(await job4.markFailed());
    t.ok(await job4.retry({ delayFor: 100000 }));
    const info4 = (await job4.getInfo())!;
    t.equal(info4.maxAttempts, 2);
    t.equal(info4.attempt, 2);
    t.ok(info4.delayUntil > info4.retriedAt);
    t.ok(await queue.getJob(id2).then((job) => job!.remove()));
    await worker.unregister();
  });

  await t.test('Queues', async (t) => {
    const id1 = await queue.addJob('add', { first: 100, second: 1 });
    const worker = await queue.getNewWorker().register();
    t.notOk(await queue.assignNextJob(worker, 0, { queueNames: ['test1'] }));
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id1);
    t.equal((await job1.getInfo())!.queueName, 'default');
    t.ok(await job1.markSucceeded());

    const id2 = await queue.addJob('add', { first: 100, second: 3 }, { queueName: 'test1' });
    t.notOk(await queue.assignNextJob(worker));
    const job2 = (await queue.assignNextJob(worker, 0, { queueNames: ['test1'] }))!;
    t.equal(job2.id, id2);
    t.equal((await job2.getInfo())!.queueName, 'test1');
    t.ok(await job2.markSucceeded());
    t.ok(await job2.retry({ queueName: 'test2' }));
    const job3 = (await queue.assignNextJob(worker, 0, { queueNames: ['default', 'test2'] }))!;
    t.equal(job3.id, id2);
    t.equal((await job3.getInfo())!.queueName, 'test2');
    t.ok(await job3.markSucceeded());
    await worker.unregister();
  });

  await t.test('Failed jobs', async (t) => {
    const id1 = await queue.addJob('add', { first: 5, second: 6 });
    const worker = await queue.getNewWorker().register();
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id1);
    t.equal(job1.progress, 0.0);
    t.equal(await job1.updateProgress(0.5), true);
    t.equal(job1.progress, 0.5);
    t.notOk((await job1.getInfo())!.result);
    t.equal((await job1.getInfo())!.progress, 0.5);
    t.ok(await job1.markFailed());
    t.notOk(await job1.markSucceeded());
    t.equal((await job1.getInfo())!.result, 'Unknown error');
    t.equal((await job1.getInfo())!.state, JobState.Failed);
    t.equal((await job1.getInfo())!.progress, 0.5);
    t.equal(job1.progress, 0.5);

    const id2 = await queue.addJob('add', { first: 6, second: 7 });
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id2);
    t.ok(await job2.markFailed('Something bad happened'));
    t.equal((await job2.getInfo())!.state, JobState.Failed);
    t.equal((await job2.getInfo())!.result, 'Something bad happened');

    const id3 = await queue.addJob('fail');
    const job3 = (await queue.assignNextJob(worker))!;
    t.equal(job3.id, id3);
    await job3.perform(worker);
    t.equal((await job3.getInfo())!.state, JobState.Failed);
    t.match((await job3.getInfo())!.result, {
      name: 'Error',
      message: /Intentional failure/,
      stack: /Intentional failure/,
    });
    await worker.unregister();
  });

  await t.test('Nested data structures', async (t) => {
    queue.registerTask('nested', async (job) => {
      const { object, array } = job.args;
      await job.amendMetadata({ bar: { baz: [1, 2, 3] } });
      await job.amendMetadata({ baz: 'yada' });
      return [{ 23: object.first[0].second + array[0][0] }];
    });
    await queue.addJob(
      'nested',
      { object: { first: [{ second: 'test' }] }, array: [[3]] },
      { metadata: { foo: [4, 5, 6] } },
    );
    const worker = await queue.getNewWorker().register();
    const job = (await queue.assignNextJob(worker))!;
    await job.perform(worker);
    t.equal((await job.getInfo())!.state, JobState.Succeeded);
    t.ok(await job.amendMetadata({ yada: ['works'] }));
    t.same((await job.getInfo())!.metadata, { foo: [4, 5, 6], bar: { baz: [1, 2, 3] }, baz: 'yada', yada: ['works'] });
    t.same((await job.getInfo())!.result, [{ 23: 'test3' }]);
    t.ok(await job.amendMetadata({ yada: null, bar: null }));
    t.same((await job.getInfo())!.metadata, { foo: [4, 5, 6], baz: 'yada' });
    t.notOk(await backend.amendJobMetadata(-1, { yada: [JobState.Failed] }));
    await worker.unregister();
  });

  await t.test('Multiple attempts with backoff while processing', async (t) => {
    const id = await queue.addJob('fail', {}, { maxAttempts: 3 });
    const worker = await queue.getNewWorker().register();
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id);
    t.equal(job1.attempt, 1);
    const info1 = (await job1.getInfo())!;
    t.equal(info1.state, JobState.Running);
    t.equal(info1.maxAttempts, 3);
    t.equal(info1.attempt, 1);
    await job1.perform(worker);
    const info2 = (await job1.getInfo())!;
    t.equal(info2.state, JobState.Scheduled);
    t.match(info2.result, { message: /Intentional failure/ });
    t.equal(info2.maxAttempts, 3);
    t.equal(info2.attempt, 2);
    t.ok(info1.retriedAt < info1.delayUntil);

    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() WHERE id = $1`, [id]); // Skip backoff

    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id);
    t.equal(job2.attempt, 2);
    const info3 = (await job2.getInfo())!;
    t.equal(info3.state, JobState.Running);
    t.equal(info3.maxAttempts, 3);
    t.equal(info3.attempt, 2);
    await job2.perform(worker);
    const info4 = (await job2.getInfo())!;
    t.equal(info4.state, JobState.Scheduled);
    t.equal(info4.maxAttempts, 3);
    t.equal(info4.attempt, 3);

    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() WHERE id = $1`, [id]); // Skip backoff again

    const job3 = (await queue.assignNextJob(worker))!;
    t.equal(job3.id, id);
    t.equal(job3.attempt, 3);
    const info5 = (await job3.getInfo())!;
    t.equal(info5.state, JobState.Running);
    t.equal(info5.maxAttempts, 3);
    t.equal(info5.attempt, 3);
    await job3.perform(worker);
    const info6 = (await job3.getInfo())!;
    t.equal(info6.state, JobState.Failed);
    t.match(info6.result, { message: /Intentional failure/ });
    t.equal(info6.maxAttempts, 3);
    t.equal(info6.attempt, 3);

    t.ok(await job3.retry({ maxAttempts: 5 }));
    const job4 = (await queue.assignNextJob(worker))!;
    t.equal(job4.id, id);
    await job4.perform(worker);
    t.equal((await job4.getInfo())!.state, JobState.Scheduled);

    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() WHERE id = $1`, [id]); // Skip backoff

    const job5 = (await queue.assignNextJob(worker))!;
    t.equal(job5.id, id);
    await job5.perform(worker);
    t.equal((await job5.getInfo())!.state, JobState.Failed);
    await worker.unregister();
  });

  await t.test('Multiple attempts with backoff during maintenance', async (t) => {
    const id = await queue.addJob('fail', {}, { maxAttempts: 2 });
    const worker = await queue.getNewWorker().register();
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id);
    t.equal(job1.attempt, 1);
    const info1 = (await job1.getInfo())!;
    t.equal(info1.state, JobState.Running);
    t.equal(info1.maxAttempts, 2);
    t.equal(info1.attempt, 1);
    await worker.unregister();
    await queue.prune();
    const info2 = (await job1.getInfo())!;
    t.equal(info2.state, JobState.Scheduled);
    t.match(info2.result, 'Worker went away');
    t.equal(info2.maxAttempts, 2);
    t.equal(info2.attempt, 2);
    t.ok(info2.retriedAt < info2.delayUntil);

    await pool.query(`UPDATE ${JOB_TABLE} SET delay_until = NOW() WHERE id = $1`, [id]); // Skip backoff

    const worker2 = await queue.getNewWorker().register();
    const job2 = (await queue.assignNextJob(worker2))!;
    t.equal(job2.id, id);
    t.equal(job2.attempt, 2);
    await worker2.unregister();
    await queue.prune();
    const info3 = (await job2.getInfo())!;
    t.equal(info3.state, JobState.Abandoned);
    t.match(info3.result, 'Worker went away');
  });

  await t.test('A job needs to be dequeued again after a retry', async (t) => {
    queue.registerTask('restart', async () => {
      return;
    });
    const id = await queue.addJob('restart');
    const worker = await queue.getNewWorker().register();
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id);
    t.ok(await job1.markSucceeded());
    t.equal((await job1.getInfo())!.state, JobState.Succeeded);
    t.ok(await job1.retry());
    t.equal((await job1.getInfo())!.state, JobState.Pending);
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal((await job2.getInfo())!.state, JobState.Running);
    t.notOk(await job1.markSucceeded());
    t.equal((await job2.getInfo())!.state, JobState.Running);
    t.equal(job2.id, id);
    t.ok(await job2.markSucceeded());
    t.notOk(await job1.retry());
    t.equal((await job2.getInfo())!.state, JobState.Succeeded);
    await worker.unregister();
  });

  await t.test('Perform jobs concurrently', async (t) => {
    const id1 = await queue.addJob('add', { first: 10, second: 11 });
    const id2 = await queue.addJob('add', { first: 12, second: 13 });
    const id3 = await queue.addJob('test');
    const id4 = await queue.addJob('fail');
    const worker = await queue.getNewWorker().register();
    const job1 = (await queue.assignNextJob(worker))!;
    const job2 = (await queue.assignNextJob(worker))!;
    const job3 = (await queue.assignNextJob(worker))!;
    const job4 = (await queue.assignNextJob(worker))!;
    await Promise.all([job1.perform(worker), job2.perform(worker), job3.perform(worker), job4.perform(worker)]);
    t.equal((await queue.getJob(id1).then((job) => job!.getInfo()))!.state, JobState.Succeeded);
    t.equal((await queue.getJob(id2).then((job) => job!.getInfo()))!.state, JobState.Succeeded);
    t.equal((await queue.getJob(id3).then((job) => job!.getInfo()))!.state, JobState.Succeeded);
    t.equal((await queue.getJob(id4).then((job) => job!.getInfo()))!.state, JobState.Failed);
    await worker.unregister();
  });

  await t.test('Job dependencies', async (t) => {
    await queue.prune({ jobRetentionPeriod: 0 });
    t.equal((await queue.getStatistics()).succeededJobs, 0);
    const worker = await queue.getNewWorker().register();
    const id1 = await queue.addJob('test');
    const id2 = await queue.addJob('test');
    const id3 = await queue.addJob('test', {}, { parentJobIds: [id1, id2] });
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id1);
    t.same((await job1.getInfo())!.childJobIds, [id3]);
    t.same((await job1.getInfo())!.parentJobIds, []);
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id2);
    t.same((await job2.getInfo())!.childJobIds, [id3]);
    t.same((await job2.getInfo())!.parentJobIds, []);
    t.notOk(await queue.assignNextJob(worker));
    t.ok(await job1.markSucceeded());
    t.notOk(await queue.assignNextJob(worker));
    t.ok(await job2.markFailed());
    t.notOk(await queue.assignNextJob(worker));
    t.ok(await job2.retry());
    const job3 = (await queue.assignNextJob(worker))!;
    t.equal(job3.id, id2);
    t.ok(await job3.markSucceeded());
    const job4 = (await queue.assignNextJob(worker))!;
    t.equal(job4.id, id3);
    t.same((await job4.getInfo())!.childJobIds, []);
    t.same((await job4.getInfo())!.parentJobIds, [id1, id2]);
    t.equal((await queue.getStatistics()).succeededJobs, 2);
    await queue.prune({ jobRetentionPeriod: 0 });
    t.equal((await queue.getStatistics()).succeededJobs, 0);
    t.ok(await job4.markSucceeded());
    t.equal((await queue.getStatistics()).succeededJobs, 1);
    await queue.prune({ jobRetentionPeriod: 0 });
    t.equal((await queue.getStatistics()).succeededJobs, 0);

    const id4 = await queue.addJob('test', {}, { parentJobIds: [-1] });
    const job5 = (await queue.assignNextJob(worker))!;
    t.equal(job5.id, id4);
    t.ok(await job5.markSucceeded());
    const id5 = await queue.addJob('test', {}, { parentJobIds: [-1] });
    const job6 = (await queue.assignNextJob(worker))!;
    t.equal(job6.id, id5);
    t.same((await job6.getInfo())!.parentJobIds, [-1]);
    t.ok(await job6.retry({ parentJobIds: [-1, -2] }));
    const job7 = (await queue.assignNextJob(worker))!;
    t.same((await job7.getInfo())!.parentJobIds, [-1, -2]);
    t.ok(await job7.markSucceeded());

    const id6 = await queue.addJob('test');
    const id7 = await queue.addJob('test');
    const id8 = await queue.addJob('test', {}, { parentJobIds: [id6, id7] });
    const child = (await queue.getJob(id8))!;
    const parents = await child.getParentJobs();
    t.equal(parents.length, 2);
    t.equal(parents[0].id, id6);
    t.equal(parents[1].id, id7);
    await parents[0].remove();
    await parents[1].remove();
    t.equal((await child.getParentJobs()).length, 0);
    t.ok(await child.remove());
    await worker.unregister();
  });

  await t.test('Job dependencies (lax)', async (t) => {
    const worker = await queue.getNewWorker().register();
    const id1 = await queue.addJob('test');
    const id2 = await queue.addJob('test');
    const id3 = await queue.addJob('test', {}, { laxDependency: true, parentJobIds: [id1, id2] });
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id1);
    t.same((await job1.getInfo())!.childJobIds, [id3]);
    t.same((await job1.getInfo())!.parentJobIds, []);
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id2);
    t.same((await job2.getInfo())!.childJobIds, [id3]);
    t.same((await job2.getInfo())!.parentJobIds, []);
    t.notOk(await queue.assignNextJob(worker));
    t.ok(await job1.markSucceeded());
    t.notOk(await queue.assignNextJob(worker));
    t.ok(await job2.markFailed());
    const job3 = (await queue.assignNextJob(worker))!;
    t.equal(job3.id, id3);
    t.same((await job3.getInfo())!.childJobIds, []);
    t.same((await job3.getInfo())!.parentJobIds, [id1, id2]);
    t.ok(await job3.markSucceeded());

    const id4 = await queue.addJob('test');
    const id5 = await queue.addJob('test', {}, { parentJobIds: [id4] });
    const job4 = (await queue.assignNextJob(worker))!;
    t.equal(job4.id, id4);
    t.notOk(await queue.assignNextJob(worker));
    t.ok(await job4.markFailed());
    t.notOk(await queue.assignNextJob(worker));
    t.ok(await queue.getJob(id5).then((job) => job!.retry({ laxDependency: true })));
    const job5 = (await queue.assignNextJob(worker))!;
    t.equal(job5.id, id5);
    t.same((await job5.getInfo())!.childJobIds, []);
    t.same((await job5.getInfo())!.parentJobIds, [id4]);
    t.ok(await job5.markSucceeded());
    t.ok(await job4.remove());

    t.same((await queue.listJobInfos({ ids: [id5] }).next())!.laxDependency, true);
    t.ok(await queue.getJob(id5).then((job) => job!.retry()));
    t.same((await queue.listJobInfos({ ids: [id5] }).next())!.laxDependency, true);
    t.ok(await queue.getJob(id5).then((job) => job!.retry({ laxDependency: false })));
    t.same((await queue.listJobInfos({ ids: [id5] }).next())!.laxDependency, false);
    t.ok(await queue.getJob(id5).then((job) => job!.retry()));
    t.same((await queue.listJobInfos({ ids: [id5] }).next())!.laxDependency, false);
    t.ok(await queue.getJob(id5).then((job) => job!.remove()));
    await worker.unregister();
  });

  await t.test('Expiring jobs', async (t) => {
    const id1 = await queue.addJob('test');
    t.notOk((await queue.getJob(id1).then((job) => job!.getInfo()))!.expiresAt);
    t.ok(await queue.getJob(id1).then((job) => job!.remove()));

    const id2 = await queue.addJob('test', {}, { expireIn: 300000 });
    t.same((await queue.getJob(id2).then((job) => job!.getInfo()))!.expiresAt instanceof Date, true);
    const worker = await queue.getNewWorker().register();
    const job1 = (await queue.assignNextJob(worker))!;
    t.equal(job1.id, id2);
    const expires = (await job1.getInfo())!.expiresAt;
    t.same(expires instanceof Date, true);
    t.ok(await job1.markSucceeded());
    t.ok(await job1.retry({ expireIn: 600000 }));
    const info = (await queue.getJob(id2).then((job) => job!.getInfo()))!;
    t.equal(info.state, JobState.Pending);
    t.same(info.expiresAt instanceof Date, true);
    t.not(info.expiresAt.getTime(), expires.getTime());
    await queue.prune();
    t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 1);
    const job2 = (await queue.assignNextJob(worker))!;
    t.equal(job2.id, id2);
    t.ok(await job2.markSucceeded());

    const id3 = await queue.addJob('test', {}, { expireIn: 300000 });
    t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 1);
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [id3]);
    await queue.prune();
    t.notOk(await queue.assignNextJob(worker));
    t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 0);

    const id4 = await queue.addJob('test', {}, { expireIn: 300000 });
    const job4 = (await queue.assignNextJob(worker))!;
    t.equal(job4.id, id4);
    t.ok(await job4.markSucceeded());
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [id4]);
    await queue.prune();
    t.equal((await job4.getInfo())!.state, JobState.Succeeded);

    const id5 = await queue.addJob('test', {}, { expireIn: 300000 });
    const job5 = (await queue.assignNextJob(worker))!;
    t.equal(job5.id, id5);
    t.ok(await job5.markFailed());
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [id5]);
    await queue.prune();
    t.equal((await job5.getInfo())!.state, JobState.Failed);

    const id6 = await queue.addJob('test', {}, { expireIn: 300000 });
    const job6 = (await queue.assignNextJob(worker))!;
    t.equal(job6.id, id6);
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [id6]);
    await queue.prune();
    t.equal((await job6.getInfo())!.state, JobState.Running);
    t.ok(await job6.markSucceeded());

    const id7 = await queue.addJob('test', {}, { expireIn: 300000 });
    const id8 = await queue.addJob('test', {}, { expireIn: 300000, parentJobIds: [id7] });
    t.notOk(await queue.assignNextJob(worker, 0, { id: id8 }));
    await pool.query(`UPDATE ${JOB_TABLE} SET expires_at = NOW() - INTERVAL '1 day' WHERE id = $1`, [id7]);
    await queue.prune();
    const job8 = (await queue.assignNextJob(worker, 0, { id: id8 }))!;
    t.ok(await job8.markSucceeded());
    await worker.unregister();
  });

  await t.test('runJobs', async (t) => {
    queue.registerTask('record_pid', async () => {
      return { pid: process.pid };
    });

    const id1 = await queue.addJob('record_pid');
    const id2 = await queue.addJob('fail');
    const id3 = await queue.addJob('record_pid');
    await queue.runJobs();
    const job1 = (await queue.getJob(id1))!;
    t.equal(job1.taskName, 'record_pid');
    t.equal((await job1.getInfo())!.state, JobState.Succeeded);
    t.same((await job1.getInfo())!.result, { pid: process.pid });
    const job2 = (await queue.getJob(id2))!;
    t.equal(job2.taskName, 'fail');
    t.equal((await job2.getInfo())!.state, JobState.Failed);
    t.match((await job2.getInfo())!.result, { message: /Intentional failure!/ });
    const job3 = (await queue.getJob(id3))!;
    t.equal(job3.taskName, 'record_pid');
    t.equal((await job3.getInfo())!.state, JobState.Succeeded);
    t.same((await job3.getInfo())!.result, { pid: process.pid });

    const id4 = await queue.addJob('record_pid');
    await queue.runJobs();
    const job4 = (await queue.getJob(id4))!;
    t.equal(job4.taskName, 'record_pid');
    t.equal((await job4.getInfo())!.state, JobState.Succeeded);
    t.same((await job4.getInfo())!.result, { pid: process.pid });
  });

  await t.test('runJob', async (t) => {
    const id1 = await queue.addJob('test', {}, { maxAttempts: 2 });
    const id2 = await queue.addJob('test');
    const id3 = await queue.addJob('test', {}, { parentJobIds: [id1, id2] });
    t.notOk(await queue.runJob(id3));

    const info1 = (await queue.getJob(id1).then((job) => job!.getInfo()))!;
    t.equal(info1.queueName, 'default');
    t.equal(info1.state, JobState.Pending);
    t.equal(info1.maxAttempts, 2);
    t.equal(info1.attempt, 1);
    t.ok(await queue.runJob(id1));
    const info2 = (await queue.getJob(id1).then((job) => job!.getInfo()))!;
    t.equal(info2.queueName, DefaultWorker.FOREGROUND_QUEUE);
    t.equal(info2.state, JobState.Succeeded);
    t.equal(info2.maxAttempts, 3);
    t.equal(info2.attempt, 2);

    t.ok(await queue.runJob(id2));
    const info3 = (await queue.getJob(id2).then((job) => job!.getInfo()))!;
    t.equal(info3.queueName, DefaultWorker.FOREGROUND_QUEUE);
    t.equal(info3.state, JobState.Succeeded);
    t.equal(info3.maxAttempts, 2);
    t.equal(info3.attempt, 2);

    t.ok(await queue.runJob(id3));
    const info4 = (await queue.getJob(id3).then((job) => job!.getInfo()))!;
    t.equal(info4.queueName, DefaultWorker.FOREGROUND_QUEUE);
    t.equal(info4.state, JobState.Succeeded);
    t.equal(info4.maxAttempts, 3);
    t.equal(info4.attempt, 3);

    t.notOk(await queue.runJob(id3 + 1));

    const id4 = await queue.addJob('fail');
    let result;
    try {
      await queue.runJob(id4);
    } catch (error) {
      result = error;
    }
    t.match(result, { message: /Intentional failure/ });
    const info5 = (await queue.getJob(id4).then((job) => job!.getInfo()))!;
    t.ok(info5.workerId);
    t.equal((await queue.getStatistics()).onlineWorkers, 0);
    t.equal(info5.maxAttempts, 2);
    t.equal(info5.attempt, 2);
    t.equal(info5.state, JobState.Failed);
    t.equal(info5.queueName, DefaultWorker.FOREGROUND_QUEUE);
    t.match(info5.result, { message: /Intentional failure/ });
  });

  await queue.end();

  // Clean up once we are done
  await pool.query('DROP SCHEMA queue_test CASCADE');

  await pool.end();
});
