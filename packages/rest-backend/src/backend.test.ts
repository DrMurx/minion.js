import { DefaultQueue, JobState, WorkerState } from '@queuebone/core';
import { createPool, PgBackend } from '@queuebone/pg-backend';
import Fastify from 'fastify';
import os from 'os';
import t from 'tap';
import { RestBackend } from './backend.js';
import { ServerQueue } from './server/queue.js';

const skip = process.env.TEST_ONLINE === undefined ? { skip: 'set TEST_ONLINE to enable this test' } : {};
const SCHEMA = 'queue_http_backend_test';
const PORT = 20595;

t.test('HTTP backend', skip, async (t) => {
  const pool = createPool(`${process.env.TEST_ONLINE!}?currentSchema=${SCHEMA}`);

  // Isolate tests
  await pool.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
  await pool.query(`CREATE SCHEMA ${SCHEMA}`);

  // Create server components
  const serverBackend = new PgBackend(pool);
  const fastify = Fastify({
    logger: false,
  });
  const serverQueue = new ServerQueue(fastify, serverBackend, {
    backoffStrategy: () => 0, // No backoff for this test
    workerProfiles: [
      {
        name: 'test-worker-1',
        token: 'test-token-1',
        config: {
          queueNames: ['default'],
          heartbeatInterval: 60 * 60 * 1000,
        },
        maxWorkers: 2,
      },
      {
        name: 'test-worker-2',
        token: 'test-token-2',
        config: {
          queueNames: ['default'],
        },
      },
    ],
  });
  await serverQueue.start();
  fastify.listen({ port: PORT });

  // Create client components
  const clientBackend = new RestBackend(`http://localhost:${PORT}`, 'test-token-1');
  const clientQueue = new DefaultQueue(clientBackend, {
    pruneEnabled: false,
  });
  await clientQueue.start();

  // Register at some simple tasks for further tests
  clientQueue.registerTask('test', async () => {
    return;
  });
  clientQueue.registerTask('fail', async () => {
    throw new Error('Intentional failure!');
  });
  clientQueue.registerTask('add', async (job) => {
    const { first, second } = job.args as any;
    return { added: first + second };
  });

  await t.test('Register worker', async (t) => {
    const results1 = await serverBackend.getWorkerInfos(0, 10, {});
    t.equal(results1.total, 0);

    const worker1 = await clientQueue.getNewWorker().register();
    const worker2 = clientQueue.getNewWorker({ metadata: { cant: 'set metadata on remote!' } });
    await worker2.register();

    try {
      // Can't register a 3rd worker
      await clientQueue.getNewWorker().register();
      t.fail();
    } catch {
      t.ok(true);
    }

    t.equal(worker1.config.heartbeatInterval, 3600000);

    const batch1 = (await serverBackend.getWorkerInfos(0, 10, {})).workers;
    t.equal(batch1[0].id, worker1.id);
    t.equal(batch1[0].config.heartbeatInterval, 3600000);
    t.equal(batch1[0].state, WorkerState.Online);
    t.same(Object.keys(batch1[0].metadata), ['host', 'name']);
    t.equal(batch1[0].metadata.name, 'test-worker-1');
    t.ok(batch1[0].metadata.host);
    t.equal(batch1[0].host, os.hostname());
    t.equal(batch1[0].pid, process.pid);
    t.equal(batch1[0].startedAt instanceof Date, true);
    t.equal(batch1[1].id, worker2.id);
    t.same(Object.keys(batch1[1].metadata), ['host', 'name']);
    t.equal(batch1[1].metadata.name, 'test-worker-1');
    t.ok(batch1[1].metadata.host);
    t.notOk(batch1[2]);

    await worker1.setMetadata('whatever', 'can not update remotely');
    t.equal(worker1.getMetadata('whatever'), undefined);

    const batch2 = (await serverBackend.getWorkerInfos(0, 10, {})).workers;
    t.equal(batch2[0].id, worker1.id);
    t.same(Object.keys(batch2[0].metadata), ['host', 'name']);
    t.equal(batch2[1].id, worker2.id);
    t.notOk(batch2[1].metadata.whatever);
    t.notOk(batch2[2]);

    await worker1.unregister();
    const batch3 = (await serverBackend.getWorkerInfos(0, 10, {})).workers;
    t.equal(batch3[0].state, WorkerState.Offline);
    t.equal(batch3[1].id, worker2.id);
    t.equal(batch3[1].state, WorkerState.Online);
    t.equal(worker1.state, WorkerState.Offline);
    t.equal(worker2.state, WorkerState.Online);

    await worker2.unregister();
    const batch4 = (await serverBackend.getWorkerInfos(0, 10, {})).workers;
    t.equal(batch4[0].state, WorkerState.Offline);
    t.equal(batch4[1].state, WorkerState.Offline);
    t.equal(worker1.state, WorkerState.Offline);
    t.equal(worker2.state, WorkerState.Offline);
  });

  await t.test('Register invalid client', async (t) => {
    const invalidClientBackend = new RestBackend(`http://localhost:${PORT}`, 'test-token-invalid');
    const invalidClientQueue = new DefaultQueue(invalidClientBackend, {
      pruneEnabled: false,
    });
    await invalidClientQueue.start();
    try {
      // Can't register any worker
      await invalidClientQueue.getNewWorker().register();
      t.fail();
    } catch {
      t.ok(true);
    }
    await invalidClientQueue.stop();
  });

  await t.test('Perform a job', async (t) => {
    const worker = await clientQueue.getNewWorker().register();

    const jobHandle1 = await serverQueue.addJob('add', { first: 17, second: 25 });
    const jobHandle2 = await serverQueue.addJob('fail', {}, { maxAttempts: 3 });
    t.equal(jobHandle1.state, JobState.Pending);
    t.equal(jobHandle2.state, JobState.Pending);

    const executor1 = (await worker.getNextExecutor())!;
    t.equal(executor1.id, jobHandle1.id);
    await jobHandle1.sync();
    t.equal(jobHandle1.state, JobState.Running);
    await executor1.perform();
    await jobHandle1.sync();
    t.equal(jobHandle1.state, JobState.Succeeded);
    t.same(jobHandle1.result, { added: 42 });

    const executor2a = (await worker.getNextExecutor())!;
    t.equal(executor2a.id, jobHandle2.id);
    await jobHandle2.sync();
    t.equal(jobHandle2.state, JobState.Running);
    await executor2a.perform();
    await jobHandle2.sync();
    t.equal(jobHandle2.state, JobState.Pending);
    t.match(jobHandle2.result, { message: /Intentional failure/ });
    t.equal(jobHandle2.maxAttempts, 3);
    t.equal(jobHandle2.attempt, 2);

    const executor2b = (await worker.getNextExecutor())!;
    t.equal(executor2b.job.id, jobHandle2.id);
    await executor2b.perform();
    await jobHandle2.sync();
    t.equal(jobHandle2.state, JobState.Pending);
    t.match(jobHandle2.result, { message: /Intentional failure/ });
    t.equal(jobHandle2.maxAttempts, 3);
    t.equal(jobHandle2.attempt, 3);

    const executor2c = (await worker.getNextExecutor())!;
    t.equal(executor2c.job.id, jobHandle2.id);
    await executor2c.perform();
    await jobHandle2.sync();
    t.equal(jobHandle2.state, JobState.Failed);
    t.match(jobHandle2.result, { message: /Intentional failure/ });
    t.equal(jobHandle2.maxAttempts, 3);
    t.equal(jobHandle2.attempt, 3);

    await worker.unregister();
  });

  await t.test('Job in concurrent worker classes', async (t) => {
    const worker = await clientQueue.getNewWorker().register();

    const clientBackend2 = new RestBackend(`http://localhost:${PORT}`, 'test-token-2');
    const clientQueue2 = new DefaultQueue(clientBackend2, {
      pruneEnabled: false,
    });
    await clientQueue2.start();
    const worker2 = await clientQueue2.getNewWorker().register();

    const jobHandle1 = await serverQueue.addJob('test');
    t.equal(jobHandle1.state, JobState.Pending);

    const executor1 = (await worker.getNextExecutor())!;
    t.equal(executor1.id, jobHandle1.id);
    await jobHandle1.sync();
    t.equal(jobHandle1.state, JobState.Running);

    t.notOk(
      await clientBackend2.markJobFinished(jobHandle1.id, jobHandle1.attempt, JobState.Succeeded, { no: 'result' }),
    );
    await jobHandle1.sync();
    t.equal(jobHandle1.state, JobState.Running);
    t.equal(jobHandle1.result, null);

    t.ok(
      await clientBackend.markJobFinished(jobHandle1.id, jobHandle1.attempt, JobState.Succeeded, { some: 'result' }),
    );
    await jobHandle1.sync();
    t.equal(jobHandle1.state, JobState.Succeeded);
    t.same(jobHandle1.result, { some: 'result' });

    await worker2.unregister();
    await clientQueue2.stop();

    await worker.unregister();
  });

  await t.test('Failed jobs', async (t) => {
    const worker = await clientQueue.getNewWorker().register();

    const jobHandle1 = await serverQueue.addJob('add', { first: 5, second: 6 });
    const job1 = (await worker.getNextExecutor())!;
    t.equal(job1.id, jobHandle1.id);
    t.equal(job1.progress, 0.0);
    t.equal(await job1.updateProgress(0.5), true);
    t.equal(job1.progress, 0.5);
    await jobHandle1.sync();
    t.notOk(jobHandle1.result);
    t.equal(jobHandle1.progress, 0.5);
    t.ok(await job1.markFailed());
    t.notOk(await job1.markSucceeded());
    await jobHandle1.sync();
    t.match(jobHandle1.result, {
      name: 'Error',
      message: 'Unknown error',
      stack: /at \w+\.markFailed/,
    });
    t.equal(jobHandle1.state, JobState.Failed);
    t.equal(jobHandle1.progress, 0.5);
    t.equal(job1.progress, 0.5);

    const jobHandle2 = await serverQueue.addJob('add', { first: 6, second: 7 });
    const job2 = (await worker.getNextExecutor())!;
    t.equal(job2.id, jobHandle2.id);
    t.ok(await job2.markFailed({ oops: 'Something bad happened' }));
    await jobHandle2.sync();
    t.equal(jobHandle2.state, JobState.Failed);
    t.same(jobHandle2.result, { oops: 'Something bad happened' });

    const jobHandle3 = await serverQueue.addJob('fail');
    const job3 = (await worker.getNextExecutor())!;
    t.equal(job3.id, jobHandle3.id);
    await job3.perform();
    await jobHandle3.sync();
    t.equal(jobHandle3.state, JobState.Failed);
    t.match(jobHandle3.result, {
      name: 'Error',
      message: /Intentional failure/,
      stack: /Intentional failure/,
    });

    await worker.unregister();
  });

  await fastify.close();
  await serverQueue.stop();
  await clientQueue.stop();

  // Clean up once we are done
  await pool.query(`DROP SCHEMA ${SCHEMA} CASCADE`);

  await pool.end();
});
