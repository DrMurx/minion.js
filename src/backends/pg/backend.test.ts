import os from 'os';
import t from 'tap';
import { DefaultQueue, type DefaultQueueInterface } from '../../queue.js';
import { type Backend } from '../../types/backend.js';
import { JobState } from '../../types/job.js';
import { WorkerState } from '../../types/worker.js';
import { PgBackend } from './backend.js';
import { createPool } from './factory.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};

t.test('PostgreSQL backend', skip, async (t) => {
  const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=queue_backend_test`);

  // Isolate tests
  await pool.query('DROP SCHEMA IF EXISTS queue_backend_test CASCADE');
  await pool.query('CREATE SCHEMA queue_backend_test');

  const backend: Backend = new PgBackend(pool);
  const queue: DefaultQueueInterface = new DefaultQueue(backend);
  await queue.updateSchema();

  // Register at some simple tasks for further tests
  queue.registerTask('test', async () => {
    return;
  });
  queue.registerTask('fail', async () => {
    throw new Error('Intentional failure!');
  });
  queue.registerTask('add', async (job) => {
    const { first, second } = job.args as any;
    return { added: first + second };
  });

  await t.test('Get worker infos', async (t) => {
    const results1b = await backend.getWorkerInfos(0, 10, {});
    t.equal(results1b.total, 0);

    const workerInfosOptions = {
      state: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
    };
    const worker1 = await queue.getNewWorker().register();
    const worker2 = queue.getNewWorker({ metadata: { whatever: 'works!' } });
    await worker2.register();

    const results2 = await backend.getWorkerInfos(0, 10, workerInfosOptions);
    t.equal(results2.total, 2);
    const batch1 = results2.workers;
    t.equal(batch1[0].id, worker1.id);
    t.equal(batch1[0].state, WorkerState.Online);
    t.equal(batch1[0].host, os.hostname());
    t.equal(batch1[0].pid, process.pid);
    t.same(batch1[0].startedAt instanceof Date, true);
    t.equal(batch1[1].id, worker2.id);
    t.equal(batch1[1].state, WorkerState.Online);
    t.equal(batch1[1].host, os.hostname());
    t.equal(batch1[1].pid, process.pid);
    t.same(batch1[1].startedAt instanceof Date, true);
    t.notOk(batch1[2]);

    const results3 = await backend.getWorkerInfos(0, 1, workerInfosOptions);
    t.equal(results3.total, 2);
    const batch2 = results3.workers;
    t.equal(batch2[0].id, worker1.id);
    t.notOk(batch2[1]);
    await worker1.setMetadata('whatever', 'works too!');
    //await worker1.register();
    const batch3 = (await backend.getWorkerInfos(0, 1, workerInfosOptions)).workers;
    t.equal(batch3[0].metadata.whatever, 'works too!');
    const batch4 = (await backend.getWorkerInfos(1, 1, workerInfosOptions)).workers;
    t.equal(batch4[0].id, worker2.id);
    t.equal(batch4[0].metadata.whatever, 'works!');
    t.notOk(batch4[1]);
    await worker1.unregister();
    await worker2.unregister();

    const results4 = await backend.getWorkerInfos(0, 10, {});
    t.equal(results4.total, 2);
  });

  await t.test('Get job infos', async (t) => {
    const worker1 = await queue.getNewWorker().register();

    t.equal((await backend.getJobInfos(1, 1, {})).total, 0);
    const addedJob1 = await queue.addJob('add');
    const addedJob2 = await queue.addJob('fail', {}, { maxAttempts: 5 });
    const addedJob3 = await queue.addJob('test', {}, { queueName: 'another_queue' });
    await queue.assignNextJob(worker1);

    const results1 = await backend.getJobInfos(0, 10, {});
    const batch1 = results1.jobs;
    t.equal(results1.total, 3);
    t.equal(batch1[0].id, addedJob1.id);
    t.equal(batch1[0].queueName, 'default');
    t.equal(batch1[0].taskName, 'add');
    t.equal(batch1[0].state, JobState.Running);
    t.equal(batch1[0].maxAttempts, 1);
    t.equal(batch1[0].attempt, 1);
    t.equal(batch1[1].id, addedJob2.id);
    t.equal(batch1[1].queueName, 'default');
    t.equal(batch1[1].taskName, 'fail');
    t.equal(batch1[1].state, JobState.Pending);
    t.equal(batch1[1].maxAttempts, 5);
    t.equal(batch1[1].attempt, 1);
    t.equal(batch1[1].id, addedJob2.id);
    t.equal(batch1[2].queueName, 'another_queue');
    t.equal(batch1[2].taskName, 'test');
    t.equal(batch1[2].state, JobState.Pending);
    t.equal(batch1[2].maxAttempts, 1);
    t.equal(batch1[2].attempt, 1);
    t.notOk(batch1[3]);

    const batch2 = (await backend.getJobInfos(0, 10, { states: [JobState.Pending] })).jobs;
    t.equal(batch2[0].id, addedJob2.id);
    t.equal(batch2[0].state, JobState.Pending);
    t.equal(batch2[1].id, addedJob3.id);
    t.equal(batch2[1].state, JobState.Pending);
    t.notOk(batch2[2]);

    const batch3 = (await backend.getJobInfos(0, 10, { taskNames: ['add'] })).jobs;
    t.equal(batch3[0].id, addedJob1.id);
    t.equal(batch3[0].taskName, 'add');
    t.equal(batch3[0].maxAttempts, 1);
    t.equal(batch3[0].attempt, 1);
    t.notOk(batch3[1]);

    const batch4 = (await backend.getJobInfos(0, 10, { taskNames: ['add', 'test'] })).jobs;
    t.equal(batch4[0].id, addedJob1.id);
    t.equal(batch4[0].taskName, 'add');
    t.equal(batch4[1].id, addedJob3.id);
    t.equal(batch4[1].taskName, 'test');
    t.notOk(batch4[2]);

    const batch5 = (await backend.getJobInfos(0, 10, { queueNames: ['default'] })).jobs;
    t.equal(batch5[0].id, addedJob1.id);
    t.equal(batch5[0].queueName, 'default');
    t.equal(batch5[1].id, addedJob2.id);
    t.equal(batch5[1].queueName, 'default');
    t.notOk(batch5[2]);

    const addedJob4 = await queue.addJob('test', {}, { metadata: { isTest: true } });
    const batch6 = (await backend.getJobInfos(0, 10, { metadata: ['isTest'] })).jobs;
    t.equal(batch6[0].id, addedJob4.id);
    t.equal(batch6[0].taskName, 'test');
    t.same(batch6[0].metadata, { isTest: true });
    t.notOk(batch6[1]);
    await (await queue.getJob(addedJob4.id))!.remove();

    const batch7 = (await backend.getJobInfos(0, 10, { queueNames: ['does_not_exist'] })).jobs;
    t.notOk(batch7[0]);

    const results4 = await backend.getJobInfos(2, 1, {});
    const batch8 = results4.jobs;
    t.equal(results4.total, 3);
    t.equal(batch8[0].id, addedJob3.id);
    t.notOk(batch8[1]);
  });

  await queue.end();

  // Clean up once we are done
  await pool.query('DROP SCHEMA queue_backend_test CASCADE');

  await pool.end();
});
