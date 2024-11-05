import os from 'os';
import t from 'tap';
import { PgBackend } from '../backends/pg/backend.js';
import { createPool } from '../backends/pg/factory.js';
import { DefaultQueue } from '../queue/queue.js';
import { type Backend } from '../types/backend.js';
import { type Job, type JobArgs, JobState } from '../types/job.js';
import { type Queue } from '../types/queue.js';
import { Task } from '../types/task.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};

t.test('Worker', skip, async (t) => {
  const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=queue_worker_test`);

  // Isolate tests
  await pool.query('DROP SCHEMA IF EXISTS queue_worker_test CASCADE');
  await pool.query('CREATE SCHEMA queue_worker_test');

  const backend: Backend = new PgBackend(pool);
  const queue: Queue = new DefaultQueue(backend, {
    // Register at least a simple task for further tests
    tasks: [
      new (class implements Task {
        readonly name = 'test';
        async handle(job: Job<JobArgs>) {
          await job.amendMetadata({ test: 'pass' });
          return { success: true };
        }
      })(),
    ],
  });
  await queue.start();

  await t.test('Register and unregister worker', async (t) => {
    const worker = await queue.getNewWorker().register();
    t.same((await worker.getInfo())!.startedAt instanceof Date, true);
    const lastSeenAt = (await worker.getInfo())!.lastSeenAt!;
    t.same(lastSeenAt instanceof Date, true);
    const id = worker.id;
    await worker.register();
    await new Promise((resolve) => setTimeout(resolve, 500));
    await worker.register();
    t.same((await worker.getInfo())!.lastSeenAt! > lastSeenAt, true);
    await worker.unregister();
    t.same(await worker.getInfo(), undefined);
    await worker.register();
    t.not(worker.id, id);
    t.equal((await worker.getInfo())!.host, os.hostname());
    await worker.unregister();
    t.same(await worker.getInfo(), undefined);
  });

  await t.test('Start worker loop and wait for job results', async (t) => {
    const worker = await queue.getNewWorker().start();
    t.equal(worker.isRunning, true);
    const job = await queue.addJob('test');

    const result = (await queue.getJobResult(job.id, { interval: 500 }))!;
    t.same(result, { success: true });
    t.ok(await job.sync());
    t.equal(job.state, JobState.Succeeded);
    t.same(job.metadata, { test: 'pass' });

    t.equal(worker.isRunning, true);
    await worker.stop();
    t.equal(worker.isRunning, false);
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

  await t.test('Worker remote control commands', async (t) => {
    const worker1 = await queue.getNewWorker().register();
    const worker1_id = worker1.id!;
    await worker1.processInbox(true);

    const worker2 = await queue.getNewWorker().register();
    const worker2_id = worker2.id!;

    let receivedCommands: unknown[] = [];
    for (const current of [worker1, worker2]) {
      current.addCommand('test_id', async (w) => {
        receivedCommands.push([w.id]);
      });
    }
    worker1.addCommand('test_args', async (w, arg) => {
      receivedCommands.push([w.id, arg]);
    });

    t.ok(await queue.sendWorkerCommand('test_id', {}, { ids: [worker1_id] }));
    t.ok(await queue.sendWorkerCommand('test_id', {}, { ids: [worker1_id, worker2_id] }));
    await worker1.processInbox(true);
    await worker2.processInbox(true);
    t.same(receivedCommands, [[worker1_id], [worker1_id], [worker2_id]]);

    receivedCommands = [];
    t.ok(await queue.sendWorkerCommand('test_id'));
    t.ok(await queue.sendWorkerCommand('test_whatever'));
    t.ok(await queue.sendWorkerCommand('test_args', { p: 23 }));
    t.ok(await queue.sendWorkerCommand('test_args', { p: 1, q: [2], r: { 3: 'three' } }, { ids: [worker1_id] }));
    await worker1.processInbox(true);
    await worker2.processInbox(true);
    t.same(receivedCommands, [
      [worker1_id],
      [worker1_id, { p: 23 }],
      [worker1_id, { p: 1, q: [2], r: { 3: 'three' } }],
      [worker2_id],
    ]);

    await worker1.unregister();
    await worker2.unregister();

    t.notOk(await queue.sendWorkerCommand('test_id'));
  });

  await queue.stop();

  // Clean up once we are done
  await pool.query('DROP SCHEMA queue_worker_test CASCADE');

  await pool.end();
});
