import os from 'os';
import t from 'tap';
import { PgBackend } from './backends/pg/backend.js';
import { createPool } from './backends/pg/factory.js';
import { DefaultQueue, type DefaultQueueInterface } from './queue.js';
import { type Backend } from './types/backend.js';
import { JobState } from './types/job.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};

t.test('Worker', skip, async (t) => {
  const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=queue_worker_test`);

  // Isolate tests
  await pool.query('DROP SCHEMA IF EXISTS queue_worker_test CASCADE');
  await pool.query('CREATE SCHEMA queue_worker_test');

  const backend: Backend = new PgBackend(pool);
  const queue: DefaultQueueInterface = new DefaultQueue(backend);
  await queue.updateSchema();

  // Register at least a simple task for further tests
  queue.registerTask('test', async (job) => {
    await job.amendMetadata({ test: 'pass' });
    return { success: true };
  });

  await t.test('Register and unregister worker', async (t) => {
    const worker = queue.getNewWorker();
    await worker.register();
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

  await t.test('Wait for job results', async (t) => {
    const worker = await queue.getNewWorker().start();
    t.equal(worker.isRunning, true);
    const job = await queue.addJob('test');

    const result = (await queue.getJobResult(job.id, { interval: 500 }))!;
    t.same(result, { success: true });
    const info = (await queue.getJobInfo(job.id))!;
    t.equal(info.state, JobState.Succeeded);
    t.same(info.metadata, { test: 'pass' });

    t.equal(worker.isRunning, true);
    await worker.stop();
    t.equal(worker.isRunning, false);
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

  await queue.end();

  // Clean up once we are done
  await pool.query('DROP SCHEMA queue_worker_test CASCADE');

  await pool.end();
});
