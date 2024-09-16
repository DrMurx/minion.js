import os from 'node:os';
import t from 'tap';
import { PgBackend } from './backends/pg/backend.js';
import { createPool } from './backends/pg/factory.js';
import { Minion } from './index.js';
import { defaultBackoffStrategy } from './minion.js';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};

t.test('Minion with PostgreSQL backend', skip, async t => {
  const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=minion_backend_test`)

  // Isolate tests
  await pool.query('DROP SCHEMA IF EXISTS minion_backend_test CASCADE');
  await pool.query('CREATE SCHEMA minion_backend_test');

  const backend = new PgBackend(pool);
  const minion = new Minion(backend);
  await minion.updateSchema();

  await t.test('Nothing to repair', async t => {
    await minion.repair();
    t.ok(minion.backend !== undefined);
  });

  await t.test('Register and unregister', async t => {
    const worker = minion.createWorker();
    await worker.register();
    t.same((await worker.getInfo())!.started instanceof Date, true);
    const notified = (await worker.getInfo())!.notified!;
    t.same(notified instanceof Date, true);
    const id = worker.id;
    await worker.register();
    await new Promise((resolve) => setTimeout(resolve, 500));
    await worker.register();
    t.same((await worker.getInfo())!.notified! > notified, true);
    await worker.unregister();
    t.same(await worker.getInfo(), null);
    await worker.register();
    t.not(worker.id, id);
    t.equal((await worker.getInfo())!.host, os.hostname());
    await worker.unregister();
    t.same(await worker.getInfo(), null);
  });

  await t.test('Job results', async t => {
    minion.addTask('test', async () => {
      return;
    });
    const worker = minion.createWorker();
    await worker.register();

    const id = await minion.addJob('test');
    const promise = minion.getJobResult(id, {interval: 0});
    const job = (await worker.dequeue(0))!;
    t.equal(job.id, id);
    t.same(await job.addNotes({foo: 'bar'}), true);
    t.same(await job.markFinished({just: 'works'}), true);
    const info = (await promise)!;
    t.same(info.result, {just: 'works'});
    t.same(info.notes, {foo: 'bar'});

    let failed;
    const id2 = await minion.addJob('test');
    t.not(id2, id);
    const promise2 = minion.getJobResult(id2, {interval: 0}).catch(reason => (failed = reason));
    const job2 = (await worker.dequeue())!;
    t.equal(job2.id, id2);
    t.not(job2.id, id);
    t.same(await job2.markFailed({just: 'works too'}), true);
    await promise2;
    t.same(failed!.result, {just: 'works too'});

    const promise3 = minion.getJobResult(id, {interval: 0});
    const info2 = (await promise3)!;
    t.same(info2.result, {just: 'works'});
    t.same(info2.notes, {foo: 'bar'});

    let finished;
    failed = undefined;
    const job3 = (await minion.getJob(id))!;
    t.same(await job3.retry(), true);
    t.equal(((await job3.getInfo())!).state, 'inactive');
    const ac = new AbortController();
    const signal = ac.signal;
    const promise4 = minion
      .getJobResult(id, {interval: 10, signal})
      .then(value => (failed = value))
      .catch(reason => (failed = reason));
    setTimeout(() => ac.abort(), 250);
    await promise4;
    t.same(finished, undefined);
    t.same(failed!.name, 'AbortError');

    finished = undefined;
    failed = undefined;
    const job4 = (await minion.getJob(id))!;
    t.same(await job4.remove(), true);
    const promise5 = minion
      .getJobResult(id, {interval: 10, signal})
      .then(value => (failed = value))
      .catch(reason => (failed = reason));
    await promise5;
    t.same(finished, null);
    t.same(failed, undefined);

    await worker.unregister();
  });

  await t.test('Wait for job', async t => {
    const worker = await minion.createWorker().register();
    setTimeout(() => minion.addJob('test'), 500);
    const job = (await worker.dequeue(10000))!;
    t.notSame(job, null);
    await job.markFinished({one: ['two', ['three']]});
    t.same((await job.getInfo())!.result, {one: ['two', ['three']]});
    await worker.unregister();
  });

  await t.test('Repair missing worker', async t => {
    const worker = await minion.createWorker().register();
    const worker2 = await minion.createWorker().register();
    t.not(worker.id, worker2.id);

    const id = await minion.addJob('test');
    const job = (await worker2.dequeue())!;
    t.equal(job.id, id);
    t.equal((await job.getInfo())!.state, 'active');
    const workerId = worker2.id;
    const missingAfter = minion.repairOptions.missingAfter + 1;
    t.ok(await worker2.getInfo());
    await pool.query("UPDATE minion_workers SET notified = NOW() - INTERVAL '1 millisecond' * $1 WHERE id = $2", [missingAfter, workerId]);

    await minion.repair();
    t.ok(!(await worker2.getInfo()));
    const info = (await job.getInfo())!;
    t.equal(info.state, 'failed');
    t.equal(info.result, 'Worker went away');
    await worker.unregister();
  });

  await t.test('Repair abandoned job', async t => {
    const worker = await minion.createWorker().register();
    await minion.addJob('test');
    const job = (await worker.dequeue())!;
    await worker.unregister();
    await minion.repair();
    const info = (await job.getInfo())!;
    t.equal(info.state, 'failed');
    t.equal(info.result, 'Worker went away');
  });

  await t.test('Repair abandoned job in minion_foreground queue (have to be handled manually)', async t => {
    const worker = await minion.createWorker().register();
    const id = await minion.addJob('test', [], {queue: 'minion_foreground'});
    const job = (await worker.dequeue(0, {queues: ['minion_foreground']}))!;
    t.equal(job.id, id);
    await worker.unregister();
    await minion.repair();
    const info = (await job.getInfo())!;
    t.equal(info.state, 'active');
    t.equal(info.queue, 'minion_foreground');
    t.same(info.result, null);
  });

  await t.test('Repair old jobs', async t => {
    t.equal(minion.repairOptions.removeAfter, 172800000);

    const worker = await minion.createWorker().register();
    const id = await minion.addJob('test');
    const id2 = await minion.addJob('test');
    const id3 = await minion.addJob('test');

    await worker.dequeue().then(job => job!.perform());
    await worker.dequeue().then(job => job!.perform());
    await worker.dequeue().then(job => job!.perform());

    const result = await pool.query('SELECT EXTRACT(EPOCH FROM finished) AS finished FROM minion_jobs WHERE id = $1', [id2]);
    const finished = result.rows[0].finished
    await pool.query('UPDATE minion_jobs SET finished = TO_TIMESTAMP($1) WHERE id = $2', [finished - (minion.repairOptions.removeAfter + 1), id2]);
    const result2 = await pool.query('SELECT EXTRACT(EPOCH FROM finished) AS finished FROM minion_jobs WHERE id = $1', [id3]);
    const finished2 = result2.rows[0].finished;
    await pool.query('UPDATE minion_jobs SET finished = TO_TIMESTAMP($1) WHERE id = $2', [finished2 - (minion.repairOptions.removeAfter + 1), id3]);

    await worker.unregister();
    await minion.repair();
    t.ok(await minion.getJob(id));
    t.ok(!(await minion.getJob(id2)));
    t.ok(!(await minion.getJob(id3)));
  });

  await t.test('Repair stuck jobs', async t => {
    t.equal(minion.repairOptions.stuckAfter, 172800000);

    const worker = await minion.createWorker().register();
    const id = await minion.addJob('test');
    const id2 = await minion.addJob('test');
    const id3 = await minion.addJob('test');
    const id4 = await minion.addJob('test');

    const stuck = minion.repairOptions.stuckAfter + 1;
    await pool.query("UPDATE minion_jobs SET delayed = NOW() - $1 * INTERVAL '1 second' WHERE id = $2", [stuck, id]);
    await pool.query("UPDATE minion_jobs SET delayed = NOW() - $1 * INTERVAL '1 second' WHERE id = $2", [stuck, id2]);
    await pool.query("UPDATE minion_jobs SET delayed = NOW() - $1 * INTERVAL '1 second' WHERE id = $2", [stuck, id3]);
    await pool.query("UPDATE minion_jobs SET delayed = NOW() - $1 * INTERVAL '1 second' WHERE id = $2", [stuck, id4]);

    const job = (await worker.dequeue(0, {id: id4}))!;
    await job.markFinished('Works!');
    const job2 = (await worker.dequeue(0, {id: id2}))!;
    await minion.repair();

    t.equal((await job2.getInfo())!.state, 'active');
    t.ok(await job2.markFinished());
    const job3 = (await minion.getJob(id))!;
    t.equal((await job3.getInfo())!.state, 'failed');
    t.equal((await job3.getInfo())!.result, 'Job appears stuck in queue');
    const job4 = (await minion.getJob(id3))!;
    t.equal((await job4.getInfo())!.state, 'failed');
    t.equal((await job4.getInfo())!.result, 'Job appears stuck in queue');
    const job5 = (await minion.getJob(id4))!;
    t.equal((await job5.getInfo())!.state, 'finished');
    t.equal((await job5.getInfo())!.result, 'Works!');
    await worker.unregister();
  });

  await t.test('List workers', async t => {
    const worker = await minion.createWorker().register();
    const worker2 = minion.createWorker();
    worker2.status.whatever = 'works!';
    await worker2.register();
    const results = await minion.backend.getWorkers(0, 10);
    t.equal(results.total, 2);

    const host = os.hostname();
    const batch = results.workers;
    t.equal(batch[0].id, worker2.id);
    t.equal(batch[0].host, host);
    t.equal(batch[0].pid, process.pid);
    t.same(batch[0].started instanceof Date, true);
    t.equal(batch[1].id, worker.id);
    t.equal(batch[1].host, host);
    t.equal(batch[1].pid, process.pid);
    t.same(batch[1].started instanceof Date, true);
    t.notOk(batch[2]);

    const results2 = await minion.backend.getWorkers(0, 1);
    const batch2 = results2.workers;
    t.equal(results2.total, 2);
    t.equal(batch2[0].id, worker2.id);
    t.equal(batch2[0].status.whatever, 'works!');
    t.notOk(batch2[1]);
    worker2.status.whatever = 'works too!';
    await worker2.register();
    const batch3 = (await minion.backend.getWorkers(0, 1)).workers;
    t.equal(batch3[0].status.whatever, 'works too!');
    const batch4 = (await minion.backend.getWorkers(1, 1)).workers;
    t.equal(batch4[0].id, worker.id);
    t.notOk(batch4[1]);
    await worker.unregister();
    await worker2.unregister();

    await minion.reset({all: true});

    const worker3 = await minion.createWorker({status: {test: 'one'}}).register();
    const worker4 = await minion.createWorker({status: {test: 'two'}}).register();
    const worker5 = await minion.createWorker({status: {test: 'three'}}).register();
    const worker6 = await minion.createWorker({status: {test: 'four'}}).register();
    const worker7 = await minion.createWorker({status: {test: 'five'}}).register();
    const workers = minion.getWorkers();
    workers.fetch = 2;
    t.notOk(workers.options.before);
    t.equal((await workers.next())!.status.test, 'five');
    t.equal(workers.options.before, 4);
    t.equal((await workers.next())!.status.test, 'four');
    t.equal((await workers.next())!.status.test, 'three');
    t.equal(workers.options.before, 2);
    t.equal((await workers.next())!.status.test, 'two');
    t.equal((await workers.next())!.status.test, 'one');
    t.equal(workers.options.before, 1);
    t.notOk(await workers.next());

    const workers1 = minion.getWorkers({ids: [2, 4, 1]});
    const result = [];
    for await (const worker of workers1) {
      result.push(worker.status.test);
    }
    t.same(result, ['four', 'two', 'one']);

    const workers2 = minion.getWorkers({ids: [2, 4, 1]});
    t.notOk(workers2.options.before);
    t.equal((await workers2.next())!.status.test, 'four');
    t.equal(workers2.options.before, 1);
    t.equal((await workers2.next())!.status.test, 'two');
    t.equal((await workers2.next())!.status.test, 'one');
    t.notOk(await workers2.next());

    const workers3 = minion.getWorkers();
    workers3.fetch = 2;
    t.equal((await workers3.next())!.status.test, 'five');
    t.equal((await workers3.next())!.status.test, 'four');
    t.equal(await workers3.numRows(), 5);
    await worker7.unregister();
    await worker6.unregister();
    await worker5.unregister();
    t.equal((await workers3.next())!.status.test, 'two');
    t.equal((await workers3.next())!.status.test, 'one');
    t.notOk(await workers3.next());
    t.equal(await workers3.numRows(), 4);
    t.equal(await minion.getWorkers().numRows(), 2);
    await worker4.unregister();
    await worker3.unregister();
  });

  await t.test('Reset (all)', async t => {
    await minion.addJob('test');
    await minion.createWorker().register();
    t.equal((await minion.backend.getJobInfos(0, 1)).total, 1);
    t.equal((await minion.backend.getWorkers(0, 1)).total, 1);
    await minion.reset({all: true});
    t.equal((await minion.backend.getJobInfos(0, 1)).total, 0);
    t.equal((await minion.backend.getWorkers(0, 1)).total, 0);
  });

  await t.test('Stats', async t => {
    minion.addTask('add', async (job, first, second) => {
      await job.markFinished({added: first + second});
    });
    minion.addTask('fail', async () => {
      throw new Error('Intentional failure!');
    });

    const stats = await minion.stats();
    t.equal(stats.workers, 0);
    t.equal(stats.active_workers, 0);
    t.equal(stats.inactive_workers, 0);
    t.equal(stats.enqueued_jobs, 0);
    t.equal(stats.active_jobs, 0);
    t.equal(stats.failed_jobs, 0);
    t.equal(stats.finished_jobs, 0);
    t.equal(stats.inactive_jobs, 0);
    t.equal(stats.delayed_jobs, 0);
    t.ok(stats.uptime);

    const worker = await minion.createWorker().register();
    t.equal((await minion.stats()).workers, 1);
    t.equal((await minion.stats()).inactive_workers, 1);
    await minion.addJob('fail');
    t.equal((await minion.stats()).enqueued_jobs, 1);
    await minion.addJob('fail');
    t.equal((await minion.stats()).enqueued_jobs, 2);
    t.equal((await minion.stats()).inactive_jobs, 2);

    const job = (await worker.dequeue(0))!;
    const stats2 = await minion.stats();
    t.equal(stats2.workers, 1);
    t.equal(stats2.active_workers, 1);
    t.equal(stats2.active_jobs, 1);
    t.equal(stats2.inactive_jobs, 1);

    await minion.addJob('fail');
    const job2 = (await worker.dequeue())!;
    const stats3 = await minion.stats();
    t.equal(stats3.active_workers, 1);
    t.equal(stats3.active_jobs, 2);
    t.equal(stats3.inactive_jobs, 1);

    t.same(await job2.markFinished(), true);
    t.same(await job.markFinished(), true);
    t.equal((await minion.stats()).finished_jobs, 2);
    const job3 = (await worker.dequeue())!;
    t.same(await job3.markFailed(), true);
    t.equal((await minion.stats()).failed_jobs, 1);
    t.same(await job3.retry(), true);
    t.equal((await minion.stats()).failed_jobs, 0);

    const job4 = (await worker.dequeue())!;
    await job4.markFinished(['works']);
    await worker.unregister();
    const stats4 = await minion.stats();
    t.equal(stats4.workers, 0);
    t.equal(stats4.active_workers, 0);
    t.equal(stats4.inactive_workers, 0);
    t.equal(stats4.active_jobs, 0);
    t.equal(stats4.failed_jobs, 0);
    t.equal(stats4.finished_jobs, 3);
    t.equal(stats4.inactive_jobs, 0);

    await worker.unregister();
  });

  await t.test('History', async t => {
    await minion.addJob('fail');
    const worker = await minion.createWorker().register();
    const job = ((await worker.dequeue()))!;
    t.ok(await job.markFailed());
    await worker.unregister();
    const history = await minion.getJobHistory();
    t.equal(history.daily.length, 24);
    t.equal(history.daily[23].finished_jobs + history.daily[22].finished_jobs, 3);
    t.equal(history.daily[23].failed_jobs + history.daily[22].failed_jobs, 1);
    t.equal(history.daily[0].finished_jobs, 0);
    t.equal(history.daily[0].failed_jobs, 0);
    t.ok(history.daily[0].epoch);
    t.ok(history.daily[1].epoch);
    t.ok(history.daily[12].epoch);
    t.ok(history.daily[23].epoch);
  });

  await t.test('List jobs', async t => {
    const id2 = await minion.addJob('add');
    t.equal((await minion.backend.getJobInfos(1, 1)).total, 5);
    const results = await minion.backend.getJobInfos(0, 10);
    const batch = results.jobs;
    t.equal(results.total, 5);
    t.ok(batch[0].id);
    t.equal(batch[0].task, 'add');
    t.equal(batch[0].state, 'inactive');
    t.equal(batch[0].retries, 0);
    t.same(batch[0].created instanceof Date, true);
    t.equal(batch[2].task, 'fail');
    t.same(batch[2].args, []);
    t.same(batch[2].notes, {});
    t.same(batch[2].result, ['works']);
    t.equal(batch[2].state, 'finished');
    t.equal(batch[2].priority, 0);
    t.same(batch[2].parents, []);
    t.same(batch[2].children, []);
    t.equal(batch[2].retries, 1);
    t.same(batch[2].created instanceof Date, true);
    t.same(batch[2].delayed instanceof Date, true);
    t.same(batch[2].finished instanceof Date, true);
    t.same(batch[2].retried instanceof Date, true);
    t.same(batch[2].started instanceof Date, true);
    t.equal(batch[3].task, 'fail');
    t.equal(batch[3].state, 'finished');
    t.equal(batch[3].retries, 0);
    t.equal(batch[4].task, 'fail');
    t.equal(batch[4].state, 'finished');
    t.equal(batch[4].retries, 0);
    t.notOk(batch[5]);

    const batch2 = (await minion.backend.getJobInfos(0, 10, {states: ['inactive']})).jobs;
    t.equal(batch2[0].state, 'inactive');
    t.equal(batch2[0].retries, 0);
    t.notOk(batch2[1]);

    const batch3 = (await minion.backend.getJobInfos(0, 10, {tasks: ['add']})).jobs;
    t.equal(batch3[0].task, 'add');
    t.equal(batch3[0].retries, 0);
    t.notOk(batch3[1]);

    const batch4 = (await minion.backend.getJobInfos(0, 10, {tasks: ['add', 'fail']})).jobs;
    t.equal(batch4[0].task, 'add');
    t.equal(batch4[1].task, 'fail');
    t.equal(batch4[2].task, 'fail');
    t.equal(batch4[3].task, 'fail');
    t.equal(batch4[4].task, 'fail');
    t.notOk(batch4[5]);

    const batch5 = (await minion.backend.getJobInfos(0, 10, {queues: ['default']})).jobs;
    t.equal(batch5[0].queue, 'default');
    t.equal(batch5[1].queue, 'default');
    t.equal(batch5[2].queue, 'default');
    t.equal(batch5[3].queue, 'default');
    t.equal(batch5[4].queue, 'default');
    t.notOk(batch5[5]);

    const id = await minion.addJob('test', [], {notes: {isTest: true}});
    const batch6 = (await minion.backend.getJobInfos(0, 10, {notes: ['isTest']})).jobs;
    t.equal(batch6[0].id, id);
    t.equal(batch6[0].task, 'test');
    t.same(batch6[0].notes, {isTest: true});
    t.notOk(batch6[1]);
    await (await minion.getJob(id))!.remove();

    const batch7 = (await minion.backend.getJobInfos(0, 10, {queues: ['does_not_exist']})).jobs;
    t.notOk(batch7[0]);

    const results4 = await minion.backend.getJobInfos(0, 1);
    const batch8 = results4.jobs;
    t.equal(results4.total, 5);
    t.equal(batch8[0].state, 'inactive');
    t.equal(batch8[0].retries, 0);
    t.notOk(batch8[1]);

    const batch9 = (await minion.backend.getJobInfos(2, 1)).jobs;
    t.equal(batch9[0].state, 'finished');
    t.equal(batch9[0].retries, 1);
    t.notOk(batch9[1]);

    const jobs = minion.getJobs();
    t.equal((await jobs.next())!.task, 'add');
    t.equal(jobs.options.before, 1);
    t.equal((await jobs.next())!.task, 'fail');
    t.equal((await jobs.next())!.task, 'fail');
    t.equal((await jobs.next())!.task, 'fail');
    t.equal((await jobs.next())!.task, 'fail');
    t.notOk(await jobs.next());
    t.equal(await jobs.numRows(), 5);

    const jobs2 = minion.getJobs({states: ['inactive']});
    t.equal(await jobs2.numRows(), 1);
    t.equal((await jobs2.next())!.task, 'add');
    t.notOk(await jobs2.next());

    const jobs3 = minion.getJobs({states: ['active']});
    t.notOk(await jobs3.next());

    const jobs4 = minion.getJobs();
    t.notOk(jobs4.options.before);
    jobs4.fetch = 2;
    t.equal((await jobs4.next())!.task, 'add');
    t.equal(jobs4.options.before, 4);
    t.equal((await jobs4.next())!.task, 'fail');
    t.equal(jobs4.options.before, 4);
    t.equal((await jobs4.next())!.task, 'fail');
    t.equal(jobs4.options.before, 2);
    t.equal((await jobs4.next())!.task, 'fail');
    t.equal(jobs4.options.before, 2);
    t.equal((await jobs4.next())!.task, 'fail');
    t.equal(jobs4.options.before, 1);
    t.notOk(await jobs4.next());
    t.equal(await jobs4.numRows(), 5);

    await minion.getJob(id2).then(job => job!.remove());
  });

  await t.test('Enqueue, dequeue and perform', async t => {
    t.notOk(await minion.getJob(12345));
    const id = await minion.addJob('add', [2, 2]);
    const info = (await minion.getJob(id).then(job => job!.getInfo()))!;
    t.same(info.args, [2, 2]);
    t.equal(info.priority, 0);
    t.equal(info.state, 'inactive');

    const worker = minion.createWorker();
    t.same(await worker.dequeue(), null);
    await worker.register();
    const job = (await worker.dequeue())!;
    t.same((await worker.getInfo())!.jobs, [id]);
    t.same((await job.getInfo())!.created instanceof Date, true);
    t.same((await job.getInfo())!.started instanceof Date, true);
    t.same((await job.getInfo())!.time instanceof Date, true);
    t.equal((await job.getInfo())!.state, 'active');
    t.same(job.args, [2, 2]);
    t.equal(job.taskName, 'add');
    t.equal(job.retries, 0);
    t.equal((await job.getInfo())!.worker, worker.id);
    t.notOk((await job.getInfo())!.finished);

    await job.perform();
    t.same((await worker.getInfo())!.jobs, []);
    t.same((await job.getInfo())!.finished instanceof Date, true);
    t.same((await job.getInfo())!.result, {added: 4});
    t.equal((await job.getInfo())!.state, 'finished');
    await worker.unregister();

    const job2 = (await minion.getJob(job.id))!;
    t.same(job2.taskName, 'add');
    t.same(job2.args, [2, 2]);
    t.equal((await job2.getInfo())!.state, 'finished');
  });

  await t.test('Retry and remove', async t => {
    const id = await minion.addJob('add', [5, 6]);
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.equal((await job.getInfo())!.attempts, 1);
    t.equal((await job.getInfo())!.retries, 0);
    t.ok(await job.markFinished());

    const job2 = (await minion.getJob(id))!;
    t.notOk((await job2.getInfo())!.retried);
    t.ok(await job.retry());
    t.same((await job2.getInfo())!.retried instanceof Date, true);
    t.equal((await job2.getInfo())!.state, 'inactive');
    t.equal((await job2.getInfo())!.retries, 1);

    const job3 = (await worker.dequeue())!;
    t.equal((await job3.getInfo())!.retries, 1);
    t.ok(await job3.retry());
    t.equal(job3.id, id);
    t.equal((await job3.getInfo())!.retries, 2);

    const job4 = (await worker.dequeue())!;
    t.equal((await job4.getInfo())!.state, 'active');
    t.ok(await job4.markFinished());
    t.ok(await job4.remove());
    t.notOk(await job4.retry());
    t.notOk(await job4.getInfo());

    const id2 = await minion.addJob('add', [6, 5]);
    const job5 = (await minion.getJob(id2))!;
    t.equal((await job5.getInfo())!.state, 'inactive');
    t.equal((await job5.getInfo())!.retries, 0);
    t.ok(await job5.retry());
    t.equal((await job5.getInfo())!.state, 'inactive');
    t.equal((await job5.getInfo())!.retries, 1);

    const job6 = (await worker.dequeue())!;
    t.equal(job6.id, id2);
    t.ok(await job6.markFailed());
    t.ok(await job6.remove());
    t.notOk(await job6.getInfo());

    const id3 = await minion.addJob('add', [5, 5]);
    const job7 = (await minion.getJob(id3))!;
    t.ok(await job7.remove());

    await worker.unregister();
  });

  await t.test('Jobs with priority', async t => {
    await minion.addJob('add', [1, 2]);
    const id = await minion.addJob('add', [2, 4], {priority: 1});
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.equal((await job.getInfo())!.priority, 1);
    t.ok(await job.markFinished());
    t.not((await worker.dequeue())!.id, id);
    const id2 = await minion.addJob('add', [2, 5]);
    const job2 = (await worker.dequeue())!;
    t.equal(job2.id, id2);
    t.equal((await job2.getInfo())!.priority, 0);
    t.ok(await job2.markFinished());
    t.ok(await job2.retry({priority: 100}));
    const job3 = (await worker.dequeue())!;
    t.equal(job3.id, id2);
    t.equal((await job3.getInfo())!.retries, 1);
    t.equal((await job3.getInfo())!.priority, 100);
    t.ok(await job3.markFinished());
    t.ok(await job3.retry({priority: 0}));
    const job4 = (await worker.dequeue())!;
    t.equal(job4.id, id2);
    t.equal((await job4.getInfo())!.retries, 2);
    t.equal((await job4.getInfo())!.priority, 0);
    t.ok(await job4.markFinished());

    const id3 = await minion.addJob('add', [2, 6], {priority: 2});
    t.notOk(await worker.dequeue(0, {minPriority: 5}));
    t.notOk(await worker.dequeue(0, {minPriority: 3}));
    const job5 = (await worker.dequeue(0, {minPriority: 2}))!;
    t.equal(job5.id, id3);
    t.equal((await job5.getInfo())!.priority, 2);
    t.ok(await job5.markFinished());
    await minion.addJob('add', [2, 8], {priority: 0});
    await minion.addJob('add', [2, 7], {priority: 5});
    await minion.addJob('add', [2, 8], {priority: -2});
    t.notOk(await worker.dequeue(0, {minPriority: 6}));
    const job6 = (await worker.dequeue(0, {minPriority: 0}))!;
    t.equal((await job6.getInfo())!.priority, 5);
    t.ok(await job6.markFinished());
    const job7 = (await worker.dequeue(0, {minPriority: 0}))!;
    t.equal((await job7.getInfo())!.priority, 0);
    t.ok(await job7.markFinished());
    t.notOk(await worker.dequeue(0, {minPriority: 0}));
    const job8 = (await worker.dequeue(0, {minPriority: -10}))!;
    t.equal((await job8.getInfo())!.priority, -2);
    t.ok(await job8.markFinished());
    await worker.unregister();
  });

  await t.test('Delayed jobs', async t => {
    const id = await minion.addJob('add', [2, 1], {delay: 100000});
    t.equal((await minion.stats()).delayed_jobs, 1);
    const worker = await minion.createWorker().register();
    t.notOk(await worker.dequeue());
    const job = (await minion.getJob(id))!;
    const info = (await job.getInfo())!;
    t.ok(info.delayed > info.created);
    await pool.query("UPDATE minion_jobs SET delayed = NOW() - INTERVAL '1 day' WHERE id = $1", [id]);
    const job2 = (await worker.dequeue())!;
    t.equal(job2.id, id);
    t.same((await job2.getInfo())!.delayed instanceof Date, true);
    t.ok(await job2.markFinished());
    t.ok(await job2.retry());
    const job3 = (await minion.getJob(id))!;
    const info2 = (await job3.getInfo())!;
    t.ok(info2.delayed <= info2.retried);
    t.ok(await job3.remove());
    t.notOk(await job3.retry());

    const id2 = await minion.addJob('add', [6, 9]);
    const job4 = (await worker.dequeue())!;
    t.equal(job4.id, id2);
    const info3 = (await job4.getInfo())!;
    t.ok(info3.delayed <= info3.created);
    t.ok(await job4.markFailed());
    t.ok(await job4.retry({delay: 100000}));
    const info4 = (await job4.getInfo())!;
    t.equal(info4.retries, 1);
    t.ok(info4.delayed > info4.retried);
    t.ok(await minion.getJob(id2).then(job => job!.remove()));
    await worker.unregister();
  });

  await t.test('Queues', async t => {
    const id = await minion.addJob('add', [100, 1]);
    const worker = await minion.createWorker().register();
    t.notOk(await worker.dequeue(0, {queues: ['test1']}));
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.equal((await job.getInfo())!.queue, 'default');
    t.ok(await job.markFinished());

    const id2 = await minion.addJob('add', [100, 3], {queue: 'test1'});
    t.notOk(await worker.dequeue());
    const job2 = (await worker.dequeue(0, {queues: ['test1']}))!;
    t.equal(job2.id, id2);
    t.equal((await job2.getInfo())!.queue, 'test1');
    t.ok(await job2.markFinished());
    t.ok(await job2.retry({queue: 'test2'}));
    const job3 = (await worker.dequeue(0, {queues: ['default', 'test2']}))!;
    t.equal(job3.id, id2);
    t.equal((await job3.getInfo())!.queue, 'test2');
    t.ok(await job3.markFinished());
    await worker.unregister();
  });

  await t.test('Failed jobs', async t => {
    const id = await minion.addJob('add', [5, 6]);
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.notOk((await job.getInfo())!.result);
    t.ok(await job.markFailed());
    t.notOk(await job.markFinished());
    t.equal((await job.getInfo())!.state, 'failed');
    t.equal((await job.getInfo())!.result, 'Unknown error');

    const id2 = await minion.addJob('add', [6, 7]);
    const job2 = (await worker.dequeue())!;
    t.equal(job2.id, id2);
    t.ok(await job2.markFailed('Something bad happened'));
    t.equal((await job2.getInfo())!.state, 'failed');
    t.equal((await job2.getInfo())!.result, 'Something bad happened');

    const id3 = await minion.addJob('fail');
    const job3 = (await worker.dequeue())!;
    t.equal(job3.id, id3);
    await job3.perform();
    t.equal((await job3.getInfo())!.state, 'failed');
    t.match((await job3.getInfo())!.result, {name: 'Error', message: /Intentional failure/, stack: /Intentional failure/});
    await worker.unregister();
  });

  await t.test('Nested data structures', async t => {
    minion.addTask('nested', async (job, object, array) => {
      await job.addNotes({bar: {baz: [1, 2, 3]}});
      await job.addNotes({baz: 'yada'});
      await job.markFinished([{23: object.first[0].second + array[0][0]}]);
    });
    await minion.addJob('nested', [{first: [{second: 'test'}]}, [[3]]], {notes: {foo: [4, 5, 6]}});
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    await job.perform();
    t.equal((await job.getInfo())!.state, 'finished');
    t.ok(await job.addNotes({yada: ['works']}));
    t.notOk(await minion.backend.addNotes(-1, {yada: ['failed']}));
    t.same((await job.getInfo())!.notes, {foo: [4, 5, 6], bar: {baz: [1, 2, 3]}, baz: 'yada', yada: ['works']});
    t.same((await job.getInfo())!.result, [{23: 'test3'}]);
    t.ok(await job.addNotes({yada: null, bar: null}));
    t.same((await job.getInfo())!.notes, {foo: [4, 5, 6], baz: 'yada'});
    await worker.unregister();
  });

  await t.test('Multiple attempts while processing', async t => {
    t.equal(defaultBackoffStrategy(0), 15);
    t.equal(defaultBackoffStrategy(1), 16);
    t.equal(defaultBackoffStrategy(2), 31);
    t.equal(defaultBackoffStrategy(3), 96);
    t.equal(defaultBackoffStrategy(4), 271);
    t.equal(defaultBackoffStrategy(5), 640);
    t.equal(defaultBackoffStrategy(25), 390640);

    const id = await minion.addJob('fail', [], {attempts: 3});
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.equal(job.retries, 0);
    const info = (await job.getInfo())!;
    t.equal(info.attempts, 3);
    t.equal(info.state, 'active');
    await job.perform();
    const info2 = (await job.getInfo())!;
    t.equal(info2.attempts, 2);
    t.equal(info2.state, 'inactive');
    t.match(info2.result, {message: /Intentional failure/});
    t.ok(info.retried < info.delayed);

    await pool.query('UPDATE minion_jobs SET delayed = NOW() WHERE id = $1', [id]);
    const job2 = (await worker.dequeue())!;
    t.equal(job2.id, id);
    t.equal(job2.retries, 1);
    const info3 = (await job2.getInfo())!;
    t.equal(info3.attempts, 2);
    t.equal(info3.state, 'active');
    await job2.perform();
    const info4 = (await job2.getInfo())!;
    t.equal(info4.attempts, 1);
    t.equal(info4.state, 'inactive');

    await pool.query('UPDATE minion_jobs SET delayed = NOW() WHERE id = $1', [id]);
    const job3 = (await worker.dequeue())!;
    t.equal(job3.id, id);
    t.equal(job3.retries, 2);
    const info5 = (await job3.getInfo())!;
    t.equal(info5.attempts, 1);
    t.equal(info5.state, 'active');
    await job3.perform();
    const info6 = (await job3.getInfo())!;
    t.equal(info6.attempts, 1);
    t.equal(info6.state, 'failed');
    t.match(info6.result, {message: /Intentional failure/});

    t.ok(await job3.retry({attempts: 2}));
    const job4 = (await worker.dequeue())!;
    t.equal(job4.id, id);
    await job4.perform();
    t.equal((await job4.getInfo())!.state, 'inactive');
    await pool.query('UPDATE minion_jobs SET delayed = NOW() WHERE id = $1', [id]);
    const job5 = (await worker.dequeue())!;
    t.equal(job5.id, id);
    await job5.perform();
    t.equal((await job5.getInfo())!.state, 'failed');
    await worker.unregister();
  });

  await t.test('Multiple attempts during maintenance', async t => {
    const id = await minion.addJob('fail', [], {attempts: 2});
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.equal(job.retries, 0);
    t.equal((await job.getInfo())!.attempts, 2);
    t.equal((await job.getInfo())!.state, 'active');
    await worker.unregister();
    await minion.repair();
    t.equal((await job.getInfo())!.state, 'inactive');
    t.match((await job.getInfo())!.result, 'Worker went away');
    t.ok((await job.getInfo())!.retried < (await job.getInfo())!.delayed);
    await pool.query('UPDATE minion_jobs SET delayed = NOW() WHERE id = $1', [id]);
    const worker2 = await minion.createWorker().register();
    const job2 = (await worker2.dequeue())!;
    t.equal(job2.id, id);
    t.equal(job2.retries, 1);
    await worker2.unregister();
    await minion.repair();
    t.equal((await job2.getInfo())!.state, 'failed');
    t.match((await job2.getInfo())!.result, 'Worker went away');
  });

  await t.test('A job needs to be dequeued again after a retry', async t => {
    minion.addTask('restart', async () => {
      return;
    });
    const id = await minion.addJob('restart');
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.ok(await job.markFinished());
    t.equal((await job.getInfo())!.state, 'finished');
    t.ok(await job.retry());
    t.equal((await job.getInfo())!.state, 'inactive');
    const job2 = (await worker.dequeue())!;
    t.equal((await job2.getInfo())!.state, 'active');
    t.notOk(await job.markFinished());
    t.equal((await job2.getInfo())!.state, 'active');
    t.equal(job2.id, id);
    t.ok(await job2.markFinished());
    t.notOk(await job.retry());
    t.equal((await job2.getInfo())!.state, 'finished');
    await worker.unregister();
  });

  await t.test('Perform jobs concurrently', async t => {
    const id = await minion.addJob('add', [10, 11]);
    const id2 = await minion.addJob('add', [12, 13]);
    const id3 = await minion.addJob('test');
    const id4 = await minion.addJob('fail');
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    const job2 = (await worker.dequeue())!;
    const job3 = (await worker.dequeue())!;
    const job4 = (await worker.dequeue())!;
    await Promise.all([job.perform(), job2.perform(), job3.perform(), job4.perform()]);
    t.equal((await minion.getJob(id).then(job => job!.getInfo()))!.state, 'finished');
    t.equal((await minion.getJob(id2).then(job => job!.getInfo()))!.state, 'finished');
    t.equal((await minion.getJob(id3).then(job => job!.getInfo()))!.state, 'finished');
    t.equal((await minion.getJob(id4).then(job => job!.getInfo()))!.state, 'failed');
    await worker.unregister();
  });

  await t.test('Job dependencies', async t => {
    await minion.repair({removeAfter: 0});
    t.equal((await minion.stats()).finished_jobs, 0);
    const worker = await minion.createWorker().register();
    const id = await minion.addJob('test');
    const id2 = await minion.addJob('test');
    const id3 = await minion.addJob('test', [], {parents: [id, id2]});
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.same((await job.getInfo())!.children, [id3]);
    t.same((await job.getInfo())!.parents, []);
    const job2 = (await worker.dequeue())!;
    t.equal(job2.id, id2);
    t.same((await job2.getInfo())!.children, [id3]);
    t.same((await job2.getInfo())!.parents, []);
    t.notOk(await worker.dequeue());
    t.ok(await job.markFinished());
    t.notOk(await worker.dequeue());
    t.ok(await job2.markFailed());
    t.notOk(await worker.dequeue());
    t.ok(await job2.retry());
    const job3 = (await worker.dequeue())!;
    t.equal(job3.id, id2);
    t.ok(await job3.markFinished());
    const job4 = (await worker.dequeue())!;
    t.equal(job4.id, id3);
    t.same((await job4.getInfo())!.children, []);
    t.same((await job4.getInfo())!.parents, [id, id2]);
    t.equal((await minion.stats()).finished_jobs, 2);
    await minion.repair({removeAfter: 0});
    t.equal((await minion.stats()).finished_jobs, 0);
    t.ok(await job4.markFinished());
    t.equal((await minion.stats()).finished_jobs, 1);
    await minion.repair({removeAfter: 0});
    t.equal((await minion.stats()).finished_jobs, 0);

    const id4 = await minion.addJob('test', [], {parents: [-1]});
    const job5 = (await worker.dequeue())!;
    t.equal(job5.id, id4);
    t.ok(await job5.markFinished());
    const id5 = await minion.addJob('test', [], {parents: [-1]});
    const job6 = (await worker.dequeue())!;
    t.equal(job6.id, id5);
    t.same((await job6.getInfo())!.parents, [-1]);
    t.ok(await job6.retry({parents: [-1, -2]}));
    const job7 = (await worker.dequeue())!;
    t.same((await job7.getInfo())!.parents, [-1, -2]);
    t.ok(await job7.markFinished());

    const id6 = await minion.addJob('test');
    const id7 = await minion.addJob('test');
    const id8 = await minion.addJob('test', [], {parents: [id6, id7]});
    const child = (await minion.getJob(id8))!;
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

  await t.test('Job dependencies (lax)', async t => {
    const worker = await minion.createWorker().register();
    const id = await minion.addJob('test');
    const id2 = await minion.addJob('test');
    const id3 = await minion.addJob('test', [], {lax: true, parents: [id, id2]});
    const job = (await worker.dequeue())!;
    t.equal(job.id, id);
    t.same((await job.getInfo())!.children, [id3]);
    t.same((await job.getInfo())!.parents, []);
    const job2 = (await worker.dequeue())!;
    t.equal(job2.id, id2);
    t.same((await job2.getInfo())!.children, [id3]);
    t.same((await job2.getInfo())!.parents, []);
    t.notOk(await worker.dequeue());
    t.ok(await job.markFinished());
    t.notOk(await worker.dequeue());
    t.ok(await job2.markFailed());
    const job3 = (await worker.dequeue())!;
    t.equal(job3.id, id3);
    t.same((await job3.getInfo())!.children, []);
    t.same((await job3.getInfo())!.parents, [id, id2]);
    t.ok(await job3.markFinished());

    const id4 = await minion.addJob('test');
    const id5 = await minion.addJob('test', [], {parents: [id4]});
    const job4 = (await worker.dequeue())!;
    t.equal(job4.id, id4);
    t.notOk(await worker.dequeue());
    t.ok(await job4.markFailed());
    t.notOk(await worker.dequeue());
    t.ok(await minion.getJob(id5).then(job => job!.retry({lax: true})));
    const job5 = (await worker.dequeue())!;
    t.equal(job5.id, id5);
    t.same((await job5.getInfo())!.children, []);
    t.same((await job5.getInfo())!.parents, [id4]);
    t.ok(await job5.markFinished());
    t.ok(await job4.remove());

    t.same((await minion.getJobs({ids: [id5]}).next())!.lax, true);
    t.ok(await minion.getJob(id5).then(job => job!.retry()));
    t.same((await minion.getJobs({ids: [id5]}).next())!.lax, true);
    t.ok(await minion.getJob(id5).then(job => job!.retry({lax: false})));
    t.same((await minion.getJobs({ids: [id5]}).next())!.lax, false);
    t.ok(await minion.getJob(id5).then(job => job!.retry()));
    t.same((await minion.getJobs({ids: [id5]}).next())!.lax, false);
    t.ok(await minion.getJob(id5).then(job => job!.remove()));
    await worker.unregister();
  });

  await t.test('Expiring jobs', async t => {
    const id = await minion.addJob('test');
    t.notOk((await minion.getJob(id).then(job => job!.getInfo()))!.expires);
    t.ok(await minion.getJob(id).then(job => job!.remove()));

    const id2 = await minion.addJob('test', [], {expire: 300000});
    t.same((await minion.getJob(id2).then(job => job!.getInfo()))!.expires instanceof Date, true);
    const worker = await minion.createWorker().register();
    const job = (await worker.dequeue())!;
    t.equal(job.id, id2);
    const expires = (await job.getInfo())!.expires;
    t.same(expires instanceof Date, true);
    t.ok(await job.markFinished());
    t.ok(await job.retry({expire: 600000}));
    const info = (await minion.getJob(id2).then(job => job!.getInfo()))!;
    t.equal(info.state, 'inactive');
    t.same(info.expires instanceof Date, true);
    t.not(info.expires.getTime(), expires.getTime());
    await minion.repair();
    t.equal(await minion.getJobs({states: ['inactive']}).numRows(), 1);
    const job2 = (await worker.dequeue())!;
    t.equal(job2.id, id2);
    t.ok(await job2.markFinished());

    const id3 = await minion.addJob('test', [], {expire: 300000});
    t.equal(await minion.getJobs({states: ['inactive']}).numRows(), 1);
    await pool.query("UPDATE minion_jobs SET expires = NOW() - INTERVAL '1 day' WHERE id = $1", [id3]);
    await minion.repair();
    t.notOk(await worker.dequeue());
    t.equal(await minion.getJobs({states: ['inactive']}).numRows(), 0);

    const id4 = await minion.addJob('test', [], {expire: 300000});
    const job4 = (await worker.dequeue())!;
    t.equal(job4.id, id4);
    t.ok(await job4.markFinished());
    await pool.query("UPDATE minion_jobs SET expires = NOW() - INTERVAL '1 day' WHERE id = $1", [id4]);
    await minion.repair();
    t.equal((await job4.getInfo())!.state, 'finished');

    const id5 = await minion.addJob('test', [], {expire: 300000});
    const job5 = (await worker.dequeue())!;
    t.equal(job5.id, id5);
    t.ok(await job5.markFailed());
    await pool.query("UPDATE minion_jobs SET expires = NOW() - INTERVAL '1 day' WHERE id = $1", [id5]);
    await minion.repair();
    t.equal((await job5.getInfo())!.state, 'failed');

    const id6 = await minion.addJob('test', [], {expire: 300000});
    const job6 = (await worker.dequeue())!;
    t.equal(job6.id, id6);
    await pool.query("UPDATE minion_jobs SET expires = NOW() - INTERVAL '1 day' WHERE id = $1", [id6]);
    await minion.repair();
    t.equal((await job6.getInfo())!.state, 'active');
    t.ok(await job6.markFinished());

    const id7 = await minion.addJob('test', [], {expire: 300000});
    const id8 = await minion.addJob('test', [], {expire: 300000, parents: [id7]});
    t.notOk(await worker.dequeue(0, {id: id8}));
    await pool.query("UPDATE minion_jobs SET expires = NOW() - INTERVAL '1 day' WHERE id = $1", [id7]);
    await minion.repair();
    const job8 = (await worker.dequeue(0, {id: id8}))!;
    t.ok(await job8.markFinished());
    await worker.unregister();
  });

  await t.test('performJobs', async t => {
    minion.addTask('record_pid', async job => {
      await job.markFinished({pid: process.pid});
    });
    minion.addTask('perform_fails', async () => {
      throw new Error('Just a test');
    });

    const id = await minion.addJob('record_pid');
    const id2 = await minion.addJob('perform_fails');
    const id3 = await minion.addJob('record_pid');
    await minion.runJobs();
    const job = (await minion.getJob(id))!;
    t.equal(job.taskName, 'record_pid');
    t.equal((await job.getInfo())!.state, 'finished');
    t.same((await job.getInfo())!.result, {pid: process.pid});
    const job2 = (await minion.getJob(id2))!;
    t.equal(job2.taskName, 'perform_fails');
    t.equal((await job2.getInfo())!.state, 'failed');
    t.match((await job2.getInfo())!.result, {message: /Just a test/});
    const job3 = (await minion.getJob(id3))!;
    t.equal(job3.taskName, 'record_pid');
    t.equal((await job3.getInfo())!.state, 'finished');
    t.same((await job3.getInfo())!.result, {pid: process.pid});

    const id4 = await minion.addJob('record_pid');
    await minion.runJobs();
    const job4 = (await minion.getJob(id4))!;
    t.equal(job4.taskName, 'record_pid');
    t.equal((await job4.getInfo())!.state, 'finished');
    t.same((await job4.getInfo())!.result, {pid: process.pid});
  });

  await t.test('Foreground', async t => {
    const id = await minion.addJob('test', [], {attempts: 2});
    const id2 = await minion.addJob('test');
    const id3 = await minion.addJob('test', [], {parents: [id, id2]});
    t.notOk(await minion.runJob(id3 + 1));
    t.notOk(await minion.runJob(id3));
    const info = (await minion.getJob(id).then(job => job!.getInfo()))!;
    t.equal(info.attempts, 2);
    t.equal(info.state, 'inactive');
    t.equal(info.queue, 'default');
    t.ok(await minion.runJob(id));
    const info2 = (await minion.getJob(id).then(job => job!.getInfo()))!;
    t.equal(info2.attempts, 1);
    t.equal(info2.retries, 1);
    t.equal(info2.state, 'finished');
    t.equal(info2.queue, 'minion_foreground');
    t.ok(await minion.runJob(id2));
    const info3 = (await minion.getJob(id2).then(job => job!.getInfo()))!;
    t.equal(info3.retries, 1);
    t.equal(info3.state, 'finished');
    t.equal(info3.queue, 'minion_foreground');

    t.ok(await minion.runJob(id3));
    const info4 = (await minion.getJob(id3).then(job => job!.getInfo()))!;
    t.equal(info4.retries, 2);
    t.equal(info4.state, 'finished');
    t.equal(info4.queue, 'minion_foreground');

    const id4 = await minion.addJob('fail');
    let result;
    try {
      await minion.runJob(id4);
    } catch (error) {
      result = error;
    }
    t.match(result, {message: /Intentional failure/});
    const info5 = (await minion.getJob(id4).then(job => job!.getInfo()))!;
    t.ok(info5.worker);
    t.equal((await minion.stats()).workers, 0);
    t.equal(info5.retries, 1);
    t.equal(info5.state, 'failed');
    t.equal(info5.queue, 'minion_foreground');
    t.match(info5.result, {message: /Intentional failure/});
  });

  await t.test('Worker remote control commands', async t => {
    const worker = await minion.createWorker().register();
    await worker.processCommands();
    const worker2 = await minion.createWorker().register();
    let commands: unknown[] = [];
    for (const current of [worker, worker2]) {
      current.addCommand('test_id', async (w, ...args) => { commands.push([w.id, ...args]); });
    }
    worker.addCommand('test_args', async (w, ...args) => { commands.push([w.id, ...args]); });
    t.ok(await minion.notifyWorkers('test_id', [], [worker.id!]));
    t.ok(await minion.notifyWorkers('test_id', [], [worker.id!, worker2.id!]));
    await worker.processCommands();
    await worker2.processCommands();
    t.same(commands, [[worker.id], [worker.id], [worker2.id]]);

    commands = [];
    t.ok(await minion.notifyWorkers('test_id'));
    t.ok(await minion.notifyWorkers('test_whatever'));
    t.ok(await minion.notifyWorkers('test_args', [23]));
    t.ok(await minion.notifyWorkers('test_args', [1, [2], {3: 'three'}], [worker.id!]));
    await worker.processCommands();
    await worker2.processCommands();
    t.same(commands, [[worker.id], [worker.id, 23], [worker.id, 1, [2], {3: 'three'}], [worker2.id]]);
    await worker.unregister();
    await worker2.unregister();
    t.notOk(await minion.notifyWorkers('test_id', []));
  });

  await minion.end();

  // Clean up once we are done
  await pool.query('DROP SCHEMA minion_backend_test CASCADE');

  await pool.end();
});
