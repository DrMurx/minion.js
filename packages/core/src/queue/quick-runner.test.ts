import { createPool, PgBackend } from '@queuebone/pg-backend';
import t from 'tap';
import { type Backend } from '../types/backend.js';
import { JobState } from '../types/job.js';
import { type Queue } from '../types/queue.js';
import { DefaultQueue } from './queue.js';
import { QuickRunner } from './quick-runner.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};
const SCHEMA = 'queue_quickrunner_test';

t.test('Queue with PostgreSQL backend', skip, async (t) => {
  const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=${SCHEMA}`);

  // Isolate tests
  await pool.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
  await pool.query(`CREATE SCHEMA ${SCHEMA}`);

  const backend: Backend = new PgBackend(pool);
  const queue: Queue = new DefaultQueue(backend, {
    // Register at some simple tasks for further tests
    tasks: {
      test: async () => {},
      fail: () => {
        throw new Error('Intentional failure!');
      },
      record_pid: async () => ({ pid: process.pid }),
    },
  });
  await queue.start();
  const quickRunner = new QuickRunner(queue);

  await t.test('runJobs', async (t) => {
    const jobHandle1 = await queue.addJob('record_pid');
    const jobHandle2 = await queue.addJob('fail');
    const jobHandle3 = await queue.addJob('record_pid');
    await quickRunner.runJobs();
    await jobHandle1.sync();
    t.equal(jobHandle1.taskName, 'record_pid');
    t.equal(jobHandle1.state, JobState.Succeeded);
    t.same(jobHandle1.result, { pid: process.pid });
    await jobHandle2.sync();
    t.equal(jobHandle2.taskName, 'fail');
    t.equal(jobHandle2.state, JobState.Failed);
    t.match(jobHandle2.result, { message: /Intentional failure!/ });
    await jobHandle3.sync();
    t.equal(jobHandle3.taskName, 'record_pid');
    t.equal(jobHandle3.state, JobState.Succeeded);
    t.same(jobHandle3.result, { pid: process.pid });

    const jobHandle4 = await queue.addJob('record_pid');
    await quickRunner.runJobs();
    await jobHandle4.sync();
    t.equal(jobHandle4.taskName, 'record_pid');
    t.equal(jobHandle4.state, JobState.Succeeded);
    t.same(jobHandle4.result, { pid: process.pid });
  });

  await t.test('runJob', async (t) => {
    const jobHandle1 = await queue.addJob('test', {}, { maxAttempts: 2 });
    const jobHandle2 = await queue.addJob('test');
    const jobHandle3 = await queue.addJob('test', {}, { parentJobIds: [jobHandle1.id, jobHandle2.id] });
    t.notOk(await quickRunner.runJob(jobHandle3.id));

    await jobHandle1.sync();
    t.equal(jobHandle1.queueName, 'default');
    t.equal(jobHandle1.state, JobState.Pending);
    t.equal(jobHandle1.maxAttempts, 2);
    t.equal(jobHandle1.attempt, 1);
    t.ok(await quickRunner.runJob(jobHandle1.id));

    await jobHandle1.sync();
    t.equal(jobHandle1.queueName, queue.FOREGROUND_QUEUE);
    t.equal(jobHandle1.state, JobState.Succeeded);
    t.equal(jobHandle1.maxAttempts, 3);
    t.equal(jobHandle1.attempt, 2);

    t.ok(await quickRunner.runJob(jobHandle2.id));
    await jobHandle2.sync();
    t.equal(jobHandle2.queueName, queue.FOREGROUND_QUEUE);
    t.equal(jobHandle2.state, JobState.Succeeded);
    t.equal(jobHandle2.maxAttempts, 2);
    t.equal(jobHandle2.attempt, 2);

    t.ok(await quickRunner.runJob(jobHandle3.id));
    await jobHandle3.sync();
    t.equal(jobHandle3.queueName, queue.FOREGROUND_QUEUE);
    t.equal(jobHandle3.state, JobState.Succeeded);
    t.equal(jobHandle3.maxAttempts, 3);
    t.equal(jobHandle3.attempt, 3);

    t.notOk(await quickRunner.runJob(jobHandle3.id + 1));

    const jobHandle4 = await queue.addJob('fail');
    let result;
    try {
      await quickRunner.runJob(jobHandle4.id);
    } catch (error) {
      result = error;
    }
    t.match(result, { message: /Intentional failure/ });
    await jobHandle4.sync();
    t.ok(jobHandle4.workerId);
    t.equal((await queue.getStatistics()).onlineWorkers, 0);
    t.equal(jobHandle4.maxAttempts, 2);
    t.equal(jobHandle4.attempt, 2);
    t.equal(jobHandle4.state, JobState.Failed);
    t.equal(jobHandle4.queueName, queue.FOREGROUND_QUEUE);
    t.match(jobHandle4.result, { message: /Intentional failure/ });
  });

  await t.test('Do not requeue abandoned job in foreground queue (have to be handled manually)', async (t) => {
    const worker = await queue.getNewWorker().register();
    const jobHandle1 = await queue.addJob('test', {}, { queueName: queue.FOREGROUND_QUEUE });
    const job = (await worker.getNextExecutor(0, { queueNames: [queue.FOREGROUND_QUEUE] }))!;
    t.equal(job.id, jobHandle1.id);
    await worker.unregister();

    await queue.prune();

    await jobHandle1.sync();
    t.equal(jobHandle1.state, JobState.Running);
    t.equal(jobHandle1.queueName, queue.FOREGROUND_QUEUE);
    t.same(jobHandle1.result, null);
  });

  await queue.stop();

  // Clean up once we are done
  await pool.query(`DROP SCHEMA ${SCHEMA} CASCADE`);

  await pool.end();
});
