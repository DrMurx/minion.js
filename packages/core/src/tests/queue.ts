import t from 'tap';
import { type JobId, JobState } from '../types/job.js';
import { type Queue } from '../types/queue.js';
import { type Task } from '../types/task.js';
import { type WorkerId, WorkerState } from '../types/worker.js';
import { DefaultJobHandle } from '../queue/job-handle.js';
import { DefaultQueue } from '../queue/queue.js';
import { Backend } from '../types/backend.js';

export interface TestableBackend {
  dateBackJobsDelayUntil(jobIds: JobId[], msBeforeNow: number): Promise<void>;
  dateBackJobExpiresAt(jobId: JobId, msBeforeNow: number): Promise<void>;
  dateBackJobFinishedAt(jobId: JobId, ms: number): Promise<void>;
  dateBackWorkerLastseenAt(workerId: WorkerId, msBeforeNow: number): Promise<void>;
}

export async function runQueueTests(backend: Backend & TestableBackend, skip: Record<string, any> = {}) {
  await t.test(`Queue with ${backend.name} backend`, skip, async (t) => {
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

      const jobHandle1 = await queue.addJob('test');
      t.ok(jobHandle1 instanceof DefaultJobHandle);

      const resultPromise1 = queue.getJobResult(jobHandle1.id, { interval: 0 });
      const executor1 = (await worker.getNextExecutor(0))!;
      const job1 = executor1.job;
      t.equal(job1.id, jobHandle1.id);
      t.same(job1.progress, 0.0);
      t.same(await job1.amendMetadata({ foo: 'bar' }), true);
      t.same(await executor1.markSucceeded({ just: 'works' }), true);
      t.same(job1.progress, 1.0);
      const result1 = (await resultPromise1)!;
      t.same(result1, { just: 'works' });
      t.ok(await jobHandle1.sync());
      t.same(jobHandle1.progress, 1.0);
      t.same(jobHandle1.metadata, { foo: 'bar' });

      let failed;
      const jobHandle2 = await queue.addJob('test');
      t.not(jobHandle2.id, jobHandle1.id);
      const promise2 = queue.getJobResult(jobHandle2.id, { interval: 0 }).catch((reason) => (failed = reason));
      const executor2 = (await worker.getNextExecutor())!;
      t.equal(executor2.id, jobHandle2.id);
      t.not(executor2.id, jobHandle1.id);
      t.same(await executor2.markFailed({ just: 'works too' }), true);
      await promise2;
      t.same(failed!.result, { just: 'works too' });

      const result2 = (await queue.getJobResult(jobHandle1.id, { interval: 0 }))!;
      t.same(result2, { just: 'works' });

      t.ok(await jobHandle1.sync());
      t.same(jobHandle1.progress, 1.0);
      t.same(jobHandle1.metadata, { foo: 'bar' });

      let succeeded;
      failed = undefined;
      const jobHandle1a = (await queue.getJob(jobHandle1.id))!;
      const jobHandle1b = await jobHandle1a.retry();
      t.ok(jobHandle1b instanceof DefaultJobHandle);
      t.equal(jobHandle1b!.state, JobState.Pending);
      const ac = new AbortController();
      const signal = ac.signal;
      const promise4 = queue
        .getJobResult(jobHandle1.id, { interval: 10, signal })
        .then((value) => (succeeded = value))
        .catch((reason) => (failed = reason));
      setTimeout(() => ac.abort(), 250);
      await promise4;
      t.same(succeeded, undefined);
      t.same(failed!.name, 'AbortError');

      succeeded = undefined;
      failed = undefined;
      const job4 = (await queue.getJob(jobHandle1.id))!;
      t.same(await job4.remove(), true);
      const promise5 = queue
        .getJobResult(jobHandle1.id, { interval: 10, signal })
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
      const jobHandle1 = (await queue.getJob(executor.id))!;
      t.same(jobHandle1.result, { one: ['two', ['three']] });
      await worker.unregister();
    });

    await t.test('Repair lost worker', async (t) => {
      const worker1 = await queue.getNewWorker().register();
      const worker2 = await queue.getNewWorker().register();
      t.not(worker1.id, worker2.id);

      const jobHandle1 = await queue.addJob('test');
      const job = (await worker2.getNextExecutor())!;
      t.equal(job.id, jobHandle1.id);
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Running);
      const lostAfter = DefaultQueue.DEFAULT_OPTIONS.workerLostTimeout + 1;
      t.ok(await queue.getWorkerInfo(worker2));

      await backend.dateBackWorkerLastseenAt(worker2.id!, lostAfter);

      await queue.prune();
      t.equal((await queue.getWorkerInfo(worker2))!.state, WorkerState.Lost);
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Abandoned);
      t.same(jobHandle1.result, { name: 'WorkerGoneError', message: 'Worker went away' });
      t.equal((await queue.getStatistics()).abandonedJobs, 1);
      await worker1.unregister();
      await worker2.unregister();
    });

    await t.test('Repair abandoned job', async (t) => {
      const worker = await queue.getNewWorker().register();
      const jobHandle1 = await queue.addJob('test');
      (await worker.getNextExecutor())!;
      await worker.unregister();

      await queue.prune();

      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Abandoned);
      t.same(jobHandle1.result, { name: 'WorkerGoneError', message: 'Worker went away' });
      t.equal((await queue.getStatistics()).abandonedJobs, 2);
    });

    await t.test('Repair old jobs', async (t) => {
      const expungePeriod = DefaultQueue.DEFAULT_OPTIONS.jobExpungePeriod;
      t.equal(expungePeriod, 172800000);

      const worker = await queue.getNewWorker().register();
      const jobHandle1 = await queue.addJob('test');
      const jobHandle2 = await queue.addJob('test');
      const jobHandle3 = await queue.addJob('test');

      await worker.getNextExecutor().then((job) => job!.perform());
      await worker.getNextExecutor().then((job) => job!.perform());
      await worker.getNextExecutor().then((job) => job!.perform());

      t.ok(await jobHandle2.sync());
      await backend.dateBackJobFinishedAt(jobHandle2.id, expungePeriod + 1);
      t.ok(await jobHandle3.sync());
      await backend.dateBackJobFinishedAt(jobHandle3.id, expungePeriod + 1);

      await worker.unregister();

      await queue.prune();

      t.ok(await queue.getJob(jobHandle1.id));
      t.notOk(await queue.getJob(jobHandle2.id));
      t.notOk(await queue.getJob(jobHandle3.id));
    });

    await t.test('Repair unattended jobs', async (t) => {
      t.equal(DefaultQueue.DEFAULT_OPTIONS.jobUnattendedPeriod, 172800000);

      const worker = await queue.getNewWorker().register();
      const jobHandle1 = await queue.addJob('test', { delayFor: 1000 });
      const jobHandle2 = await queue.addJob('test', { delayFor: 1000 });
      const jobHandle3 = await queue.addJob('test', { delayFor: 1000 });
      const jobHandle4 = await queue.addJob('test', { delayFor: 1000 });

      const unattendedPeriod = DefaultQueue.DEFAULT_OPTIONS.jobUnattendedPeriod + 1;
      await backend.dateBackJobsDelayUntil(
        [jobHandle1.id, jobHandle2.id, jobHandle3.id, jobHandle4.id],
        unattendedPeriod,
      );

      const job1 = (await worker.getNextExecutor(0, { id: jobHandle4.id }))!;
      await job1.markSucceeded({ i_say: 'Works!' });
      const job2 = (await worker.getNextExecutor(0, { id: jobHandle2.id }))!;
      await queue.prune();

      t.ok(await jobHandle2.sync());
      t.equal(jobHandle2.state, JobState.Running);
      t.ok(await job2.markSucceeded());

      t.equal((await queue.getStatistics()).unattendedJobs, 2);
      t.ok(await jobHandle1.sync());
      t.equal(jobHandle1.state, JobState.Unattended);
      t.ok(await jobHandle3.sync());
      t.equal(jobHandle3.state, JobState.Unattended);

      t.ok(await jobHandle4.sync());
      t.equal(jobHandle4.state, JobState.Succeeded);
      t.same(jobHandle4.result, { i_say: 'Works!' });

      await worker.unregister();
    });

    await t.test('List jobs', async (t) => {
      await queue.resetQueue();

      const worker = await queue.getNewWorker().register();
      const jobHandle1 = await queue.addJob('test');
      const jobHandle2 = await queue.addJob('test');
      const jobHandle3 = await queue.addJob('test');
      const jobHandle4 = await queue.addJob('test');
      const jobHandle5 = await queue.addJob('test');

      const job1 = (await worker.getNextExecutor(0))!;
      const job2 = (await worker.getNextExecutor(0))!;
      t.same(await job2.markSucceeded(), true);
      t.same(await job1.markSucceeded(), true);
      const job3 = (await worker.getNextExecutor(0))!;
      t.same(await job3.markFailed(), true);
      t.ok(await jobHandle3.retry());
      const job3a = (await worker.getNextExecutor(0))!;
      await job3a.markSucceeded({ it: 'works' });
      await worker.getNextExecutor(0);
      await worker.unregister();

      await t.test('Simple list with default chunk size', async (t) => {
        const jobs1 = queue.listJobInfos();
        t.equal(await jobs1.numRows(), 5);
        t.equal((await jobs1.next())!.id, jobHandle1.id);
        t.equal(jobs1.highestId, jobHandle5.id);
        t.equal((await jobs1.next())!.id, jobHandle2.id);
        t.equal((await jobs1.next())!.id, jobHandle3.id);
        t.equal((await jobs1.next())!.id, jobHandle4.id);
        t.equal((await jobs1.next())!.id, jobHandle5.id);
        t.notOk(await jobs1.next());
      });

      await t.test('List with filters', async (t) => {
        const jobs2 = queue.listJobInfos({ states: [JobState.Pending] });
        t.equal(await jobs2.numRows(), 1);
        t.equal((await jobs2.next())!.id, jobHandle5.id);
        t.notOk(await jobs2.next());

        const jobs3 = queue.listJobInfos({ states: [JobState.Running] });
        t.equal(await jobs3.numRows(), 1);
        t.equal((await jobs3.next())!.id, jobHandle4.id);
        t.notOk(await jobs3.next());
      });

      await t.test('List with small chunk size', async (t) => {
        const jobs4 = queue.listJobInfos({}, 2);
        t.notOk(jobs4.highestId);
        t.equal((await jobs4.next())!.id, jobHandle1.id);
        t.equal(jobs4.highestId, jobHandle2.id);
        t.equal((await jobs4.next())!.id, jobHandle2.id);
        t.equal(jobs4.highestId, jobHandle2.id);
        t.equal((await jobs4.next())!.id, jobHandle3.id);
        t.equal(jobs4.highestId, jobHandle4.id);
        t.equal((await jobs4.next())!.id, jobHandle4.id);
        t.equal(jobs4.highestId, jobHandle4.id);
        t.equal((await jobs4.next())!.id, jobHandle5.id);
        t.equal(jobs4.highestId, jobHandle5.id);
        t.notOk(await jobs4.next());
        t.equal(await jobs4.numRows(), 5);
      });
    });

    await t.test('Enqueue, dequeue and perform', async (t) => {
      await queue.resetQueue();

      t.notOk(await queue.getJob(12345));

      const jobHandle1 = await queue.addJob('add', { first: 2, second: 2 });
      t.same(jobHandle1.args, { first: 2, second: 2 });
      t.equal(jobHandle1.state, JobState.Pending);
      t.equal(jobHandle1.priority, 0);

      const worker = queue.getNewWorker();
      t.same(await worker.getNextExecutor(), null);

      await worker.register();
      const executor1 = (await worker.getNextExecutor())!;
      const job1 = executor1.job;
      t.same((await queue.getWorkerInfo(worker))!.jobIds, [jobHandle1.id]);
      t.equal(job1.taskName, 'add');
      t.equal(job1.attempt, 1);
      t.same(executor1.job.args, { first: 2, second: 2 });
      t.ok(await jobHandle1.sync());
      t.equal(jobHandle1.state, JobState.Running);
      t.equal(jobHandle1.workerId, worker.id);
      t.same(jobHandle1.createdAt instanceof Date, true);
      t.same(jobHandle1.startedAt instanceof Date, true);
      t.notOk(jobHandle1.finishedAt);
      t.same(jobHandle1.time instanceof Date, true);

      await executor1.perform();
      t.same((await queue.getWorkerInfo(worker))!.jobIds, []);
      t.ok(await jobHandle1.sync());
      t.equal(jobHandle1.state, JobState.Succeeded);
      t.same(jobHandle1.result, { added: 4 });
      t.same(jobHandle1.finishedAt instanceof Date, true);
      await worker.unregister();

      const jobHandle1b = (await queue.getJob(executor1.id))!;
      t.same(jobHandle1b.taskName, 'add');
      t.same(jobHandle1b.args, { first: 2, second: 2 });
      t.equal(jobHandle1b.state, JobState.Succeeded);
    });

    await t.test('Cancel job', async (t) => {
      const worker = await queue.getNewWorker().register();

      const jobHandle1 = await queue.addJob('add', { first: 11, second: 17 }, { delayFor: 10000 });
      t.notOk(await worker.getNextExecutor());
      t.equal(jobHandle1.state, JobState.Scheduled);
      t.ok(await jobHandle1.cancel());
      t.equal(jobHandle1.state, JobState.Canceled);

      const jobHandle2 = await queue.addJob('add', { first: 13, second: 29 });
      const job2 = (await worker.getNextExecutor())!;
      t.equal(job2.id, jobHandle2.id);
      await jobHandle2.sync();
      t.equal(jobHandle2.state, JobState.Running);
      t.notOk(await jobHandle2.cancel());
      await jobHandle2.sync();
      t.equal(jobHandle2.state, JobState.Running);

      const jobHandle3 = await queue.addJob('add', { first: 17, second: 29 });
      const job3 = (await worker.getNextExecutor())!;
      t.equal(job3.id, jobHandle3.id);
      await job3.markSucceeded();
      await jobHandle3.sync();
      t.equal(jobHandle3.state, JobState.Succeeded);
      t.notOk(await jobHandle3.cancel());
      await jobHandle3.sync();
      t.equal(jobHandle3.state, JobState.Succeeded);

      await worker.unregister();
    });

    await t.test('Retry and remove job', async (t) => {
      const jobHandle1 = await queue.addJob('add', { first: 5, second: 6 });
      const worker = await queue.getNewWorker().register();
      const executor1 = (await worker.getNextExecutor())!;
      t.equal(executor1.id, jobHandle1.id);
      await jobHandle1.sync();
      t.equal(jobHandle1.maxAttempts, 1);
      t.equal(executor1.job.attempt, 1);
      t.ok(await executor1.markSucceeded());

      t.ok(await jobHandle1.sync());
      t.notOk(jobHandle1.retriedAt);
      const jobHandle1a = (await jobHandle1.retry())!;
      t.equal(jobHandle1a.state, JobState.Pending);
      t.equal(jobHandle1a.maxAttempts, 2);
      t.equal(jobHandle1a.attempt, 2);
      t.same(jobHandle1a.retriedAt instanceof Date, true);

      const executor3 = (await worker.getNextExecutor())!;
      await jobHandle1a.sync();
      t.equal(executor3.id, jobHandle1.id);
      t.equal(jobHandle1a.maxAttempts, 2);
      t.equal(executor3.job.attempt, 2);
      const jobHandle1b = (await jobHandle1a.retry())!;
      t.equal(jobHandle1b.maxAttempts, 3);
      t.equal(jobHandle1b.attempt, 3);

      const job4 = (await worker.getNextExecutor())!;
      t.equal(job4.id, jobHandle1.id);
      t.equal(job4.state, JobState.Running);
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Running);
      t.ok(await job4.markSucceeded());
      t.ok(await jobHandle1.remove());
      t.notOk(await jobHandle1.retry());
      t.notOk(await job4.markSucceeded());

      const jobHandle2 = await queue.addJob('add', { first: 6, second: 5 });
      t.equal(jobHandle2.state, JobState.Pending);
      t.equal(jobHandle2.maxAttempts, 1);
      t.equal(jobHandle2.attempt, 1);
      t.ok(await jobHandle2.retry());
      await jobHandle2.sync();
      t.equal(jobHandle2.state, JobState.Pending);
      t.equal(jobHandle2.maxAttempts, 2);
      t.equal(jobHandle2.attempt, 2);

      const job6 = (await worker.getNextExecutor())!;
      t.equal(job6.id, jobHandle2.id);
      t.ok(await job6.markFailed({ oopsie: 'Fail and remove immediately' }));
      t.ok(await jobHandle2.remove());
      t.notOk(await job6.markFailed());

      const jobHandle3 = await queue.addJob('add', { first: 5, second: 5 });
      await queue.getJob(jobHandle3.id);
      t.ok(await jobHandle3.remove());

      await worker.unregister();
    });

    await t.test('Jobs with priority', async (t) => {
      await queue.addJob('add', { first: 1, second: 2 });
      const jobHandle1 = await queue.addJob('add', { first: 2, second: 4 }, { priority: 1 });
      const worker = await queue.getNewWorker().register();
      const job1 = (await worker.getNextExecutor())!;
      t.equal(job1.id, jobHandle1.id);
      await jobHandle1.sync();
      t.equal(jobHandle1.priority, 1);
      t.equal(jobHandle1.maxAttempts, 1);
      t.equal(jobHandle1.attempt, 1);
      t.ok(await job1.markSucceeded());
      t.not((await worker.getNextExecutor())!.id, jobHandle1.id);
      const jobHandle2 = await queue.addJob('add', { first: 2, second: 5 });
      const job2 = (await worker.getNextExecutor())!;
      t.equal(job2.id, jobHandle2.id);
      t.equal(jobHandle2.priority, 0);
      t.ok(await job2.markSucceeded());
      const jobHandle2a = (await jobHandle2.retry({ priority: 100 }))!;
      const job3 = (await worker.getNextExecutor())!;
      t.equal(job3.id, jobHandle2a.id);
      t.equal(jobHandle2a.priority, 100);
      t.equal(jobHandle2a.maxAttempts, 2);
      t.equal(jobHandle2a.attempt, 2);
      t.ok(await job3.markSucceeded());
      const jobHandle2b = (await jobHandle2a.retry({ priority: 0 }))!;
      const job4 = (await worker.getNextExecutor())!;
      t.equal(job4.id, jobHandle2b.id);
      t.equal(jobHandle2b.priority, 0);
      t.equal(jobHandle2b.maxAttempts, 3);
      t.equal(jobHandle2b.attempt, 3);
      t.ok(await job4.markSucceeded());

      const jobHandle3 = await queue.addJob('add', { first: 2, second: 6 }, { priority: 2 });
      t.notOk(await worker.getNextExecutor(0, { minPriority: 5 }));
      t.notOk(await worker.getNextExecutor(0, { minPriority: 3 }));
      const job5 = (await worker.getNextExecutor(0, { minPriority: 2 }))!;
      t.equal(job5.id, jobHandle3.id);
      await jobHandle3.sync();
      t.equal(jobHandle3.priority, 2);
      t.ok(await job5.markSucceeded());
      const jobHandle4 = await queue.addJob('add', { first: 2, second: 8 }, { priority: 0 });
      const jobHandle5 = await queue.addJob('add', { first: 2, second: 7 }, { priority: 5 });
      const jobHandle6 = await queue.addJob('add', { first: 2, second: 8 }, { priority: -2 });
      t.notOk(await worker.getNextExecutor(0, { minPriority: 6 }));
      const job6 = (await worker.getNextExecutor(0, { minPriority: 0 }))!;
      t.equal(job6.id, jobHandle5.id);
      t.ok(await job6.markSucceeded());
      const job7 = (await worker.getNextExecutor(0, { minPriority: 0 }))!;
      t.equal(job7.id, jobHandle4.id);
      t.ok(await job7.markSucceeded());
      t.notOk(await worker.getNextExecutor(0, { minPriority: 0 }));
      const job8 = (await worker.getNextExecutor(0, { minPriority: -10 }))!;
      t.equal(job8.id, jobHandle6.id);
      t.ok(await job8.markSucceeded());
      await worker.unregister();
    });

    await t.test('Delayed jobs', async (t) => {
      const jobHandle1 = await queue.addJob('add', { first: 2, second: 1 }, { delayFor: 100000 });

      t.equal((await queue.getStatistics()).scheduledJobs, 1);
      const worker = await queue.getNewWorker().register();
      t.notOk(await worker.getNextExecutor());
      t.ok(await jobHandle1.sync());
      t.ok(jobHandle1.delayUntil > jobHandle1.createdAt);
      await backend.dateBackJobsDelayUntil([jobHandle1.id], 86400 * 1000);
      const job2 = (await worker.getNextExecutor())!;
      t.equal(job2.id, jobHandle1.id);
      t.ok(await job2.markSucceeded());
      t.ok(await jobHandle1.retry());
      await jobHandle1.sync();
      t.ok(jobHandle1.delayUntil <= jobHandle1.retriedAt!);
      t.ok(await jobHandle1.remove());
      t.notOk(await jobHandle1.retry());

      const jobHandle2 = await queue.addJob('add', { first: 6, second: 9 });
      const job4 = (await worker.getNextExecutor())!;
      t.equal(job4.id, jobHandle2.id);
      await jobHandle2.sync();
      t.ok(jobHandle2.delayUntil <= jobHandle2.createdAt);
      t.ok(await job4.markFailed());
      t.ok(await jobHandle2.retry({ delayFor: 100000 }));
      await jobHandle2.sync();
      t.equal(jobHandle2.maxAttempts, 2);
      t.equal(jobHandle2.attempt, 2);
      t.ok(jobHandle2.delayUntil > jobHandle2.retriedAt!);
      t.ok(await queue.getJob(jobHandle2.id).then((job) => job!.remove()));

      await worker.unregister();
    });

    await t.test('Queues', async (t) => {
      const jobHandle1 = await queue.addJob('add', { first: 100, second: 1 });
      const worker = await queue.getNewWorker().register();
      t.notOk(await worker.getNextExecutor(0, { queueNames: 'test1' }));
      const job1 = (await worker.getNextExecutor())!;
      t.equal(job1.id, jobHandle1.id);
      await jobHandle1.sync();
      t.equal(jobHandle1.queueName, 'default');
      t.ok(await job1.markSucceeded());

      const jobHandle2 = await queue.addJob('add', { first: 100, second: 3 }, { queueName: 'test1' });
      t.notOk(await worker.getNextExecutor());
      const job2 = (await worker.getNextExecutor(0, { queueNames: 'test1' }))!;
      t.equal(job2.id, jobHandle2.id);
      await jobHandle2.sync();
      t.equal(jobHandle2.queueName, 'test1');
      t.ok(await job2.markSucceeded());
      t.ok(await jobHandle2.retry({ queueName: 'test2' }));
      const job3 = (await worker.getNextExecutor(0, { queueNames: ['default', 'test2'] }))!;
      t.equal(job3.id, jobHandle2.id);
      await jobHandle2.sync();
      t.equal(jobHandle2.queueName, 'test2');
      t.ok(await job3.markSucceeded());
      await worker.unregister();
    });

    await t.test('Failed jobs', async (t) => {
      const jobHandle1 = await queue.addJob('add', { first: 5, second: 6 });
      const worker = await queue.getNewWorker().register();
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

      const jobHandle2 = await queue.addJob('add', { first: 6, second: 7 });
      const job2 = (await worker.getNextExecutor())!;
      t.equal(job2.id, jobHandle2.id);
      t.ok(await job2.markFailed({ oops: 'Something bad happened' }));
      await jobHandle2.sync();
      t.equal(jobHandle2.state, JobState.Failed);
      t.same(jobHandle2.result, { oops: 'Something bad happened' });

      const jobHandle3 = await queue.addJob('fail');
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

    await t.test('Nested data structures', async (t) => {
      queue.registerTask('nested', async (job) => {
        const { object, array } = job.args as any;
        await job.amendMetadata({ bar: { baz: [1, 2, 3] } });
        await job.amendMetadata({ baz: 'yada' });
        return [{ 23: object.first[0].second + array[0][0] }];
      });
      const jobHandle1 = await queue.addJob(
        'nested',
        { object: { first: [{ second: 'test' }] }, array: [[3]] },
        { metadata: { foo: [4, 5, 6] } },
      );
      const worker = await queue.getNewWorker().register();
      const job = (await worker.getNextExecutor())!;
      await job.perform();
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Succeeded);

      t.ok(await job.amendMetadata({ yada: ['works'] }));
      await jobHandle1.sync();
      t.same(jobHandle1.metadata, { foo: [4, 5, 6], bar: { baz: [1, 2, 3] }, baz: 'yada', yada: ['works'] });
      t.same(jobHandle1.result, [{ 23: 'test3' }]);

      t.ok(await jobHandle1.amendMetadata({ foo: [4, 5, 6, 7], so: true }));
      t.same(jobHandle1.metadata, {
        foo: [4, 5, 6, 7],
        bar: { baz: [1, 2, 3] },
        baz: 'yada',
        yada: ['works'],
        so: true,
      });

      t.ok(await job.amendMetadata({ yada: null, bar: null }));
      await jobHandle1.sync();
      t.same(jobHandle1.metadata, { foo: [4, 5, 6, 7], baz: 'yada', so: true });

      t.notOk(await backend.amendJobMetadata(-1, 1, { yada: [JobState.Failed] }));

      await worker.unregister();
    });

    await t.test('Multiple attempts with backoff while processing', async (t) => {
      const jobHandle1 = await queue.addJob('fail', {}, { maxAttempts: 3 });
      const worker = await queue.getNewWorker().register();
      const executor1 = (await worker.getNextExecutor())!;
      const job1 = executor1.job;
      t.equal(executor1.id, jobHandle1.id);
      t.equal(job1.attempt, 1);
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Running);
      t.equal(jobHandle1.maxAttempts, 3);
      t.equal(jobHandle1.attempt, 1);
      await executor1.perform();
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Scheduled);
      t.match(jobHandle1.result, { message: /Intentional failure/ });
      t.equal(jobHandle1.maxAttempts, 3);
      t.equal(jobHandle1.attempt, 2);
      t.ok(jobHandle1.retriedAt! < jobHandle1.delayUntil);

      await backend.dateBackJobsDelayUntil([jobHandle1.id], 0); // Skip backoff

      const executor2 = (await worker.getNextExecutor())!;
      const job2 = executor2.job;
      t.equal(executor2.id, jobHandle1.id);
      t.equal(job2.attempt, 2);
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Running);
      t.equal(jobHandle1.maxAttempts, 3);
      t.equal(jobHandle1.attempt, 2);
      await executor2.perform();
      await new Promise((resolve) => setTimeout(resolve, 10));
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Scheduled);
      t.equal(jobHandle1.maxAttempts, 3);
      t.equal(jobHandle1.attempt, 3);

      await backend.dateBackJobsDelayUntil([jobHandle1.id], 0); // Skip backoff again

      const executor3 = (await worker.getNextExecutor())!;
      const job3 = executor3.job;
      t.equal(executor3.id, jobHandle1.id);
      t.equal(job3.attempt, 3);
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Running);
      t.equal(jobHandle1.maxAttempts, 3);
      t.equal(jobHandle1.attempt, 3);
      await executor3.perform();
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Failed);
      t.match(jobHandle1.result, { message: /Intentional failure/ });
      t.equal(jobHandle1.maxAttempts, 3);
      t.equal(jobHandle1.attempt, 3);

      t.ok(await jobHandle1.sync());
      t.ok(await jobHandle1.retry({ maxAttempts: 5 }));
      const job4 = (await worker.getNextExecutor())!;
      t.equal(job4.id, jobHandle1.id);
      await job4.perform();
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Scheduled);

      await backend.dateBackJobsDelayUntil([jobHandle1.id], 0); // Skip backoff

      const job5 = (await worker.getNextExecutor())!;
      t.equal(job5.id, jobHandle1.id);
      await job5.perform();
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Failed);

      await worker.unregister();
    });

    await t.test('Multiple attempts with backoff during maintenance', async (t) => {
      const jobHandle1 = await queue.addJob('fail', {}, { maxAttempts: 2 });
      const worker = await queue.getNewWorker().register();
      const executor1 = (await worker.getNextExecutor())!;
      t.equal(executor1.id, jobHandle1.id);
      t.equal(executor1.job.attempt, 1);
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Running);
      t.equal(jobHandle1.maxAttempts, 2);
      t.equal(jobHandle1.attempt, 1);
      await worker.unregister();

      await queue.prune();
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Pending);
      t.same(jobHandle1.result, { name: 'WorkerGoneError', message: 'Worker went away' });
      t.equal(jobHandle1.maxAttempts, 2);
      t.equal(jobHandle1.attempt, 2);

      await backend.dateBackJobsDelayUntil([jobHandle1.id], 0); // Skip backoff

      const worker2 = await queue.getNewWorker().register();
      const executor2 = (await worker2.getNextExecutor())!;
      t.equal(executor2.id, jobHandle1.id);
      t.equal(executor2.job.attempt, 2);
      await worker2.unregister();
      await queue.prune();
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Abandoned);
      t.same(jobHandle1.result, { name: 'WorkerGoneError', message: 'Worker went away' });
    });

    await t.test('A job needs to be dequeued again after a retry', async (t) => {
      queue.registerTask('restart', async () => {
        return;
      });
      const jobHandle1 = await queue.addJob('restart');
      const worker = await queue.getNewWorker().register();
      const executor1 = (await worker.getNextExecutor())!;
      t.equal(executor1.id, jobHandle1.id);
      t.ok(await executor1.markSucceeded());
      await jobHandle1.sync();
      t.equal(jobHandle1.state, JobState.Succeeded);
      const jobHandle2 = (await jobHandle1.retry())!;
      t.equal(jobHandle2.state, JobState.Pending);

      const executor2 = (await worker.getNextExecutor())!;
      t.equal(executor2.id, jobHandle1.id);
      await jobHandle2.sync();
      t.equal(jobHandle2.state, JobState.Running);
      t.notOk(await executor1.markSucceeded());
      await jobHandle2.sync();
      t.equal(jobHandle2.state, JobState.Running);
      t.ok(await executor2.markSucceeded());
      t.notOk(await jobHandle1.retry());
      await jobHandle2.sync();
      t.equal(jobHandle2.state, JobState.Succeeded);
      await worker.unregister();
    });

    await t.test('Perform jobs concurrently', async (t) => {
      const jobHandle1 = await queue.addJob('add', { first: 10, second: 11 });
      const jobHandle2 = await queue.addJob('add', { first: 12, second: 13 });
      const jobHandle3 = await queue.addJob('test');
      const jobHandle4 = await queue.addJob('fail');
      const worker = await queue.getNewWorker().register();
      const job1 = (await worker.getNextExecutor())!;
      const job2 = (await worker.getNextExecutor())!;
      const job3 = (await worker.getNextExecutor())!;
      const job4 = (await worker.getNextExecutor())!;
      await Promise.all([job1.perform(), job2.perform(), job3.perform(), job4.perform()]);
      await Promise.all([jobHandle1.sync(), jobHandle2.sync(), jobHandle3.sync(), jobHandle4.sync()]);
      t.equal(jobHandle1.state, JobState.Succeeded);
      t.equal(jobHandle2.state, JobState.Succeeded);
      t.equal(jobHandle3.state, JobState.Succeeded);
      t.equal(jobHandle4.state, JobState.Failed);
      await worker.unregister();
    });

    await t.test('Job dependencies', async (t) => {
      await queue.prune({ jobExpungePeriod: 0 });
      t.equal((await queue.getStatistics()).succeededJobs, 0);
      const worker = await queue.getNewWorker().register();

      const jobHandle1 = await queue.addJob('test');
      const jobHandle2 = await queue.addJob('test');
      const jobHandle3 = await queue.addJob('test', {}, { parentJobIds: [jobHandle1.id, jobHandle2.id] });
      const job1 = (await worker.getNextExecutor())!;
      t.equal(job1.id, jobHandle1.id);
      await jobHandle1.sync();
      t.same(await jobHandle1.getChildJobIds(), [jobHandle3.id]);
      t.same(jobHandle1.parentJobIds, []);
      const job2 = (await worker.getNextExecutor())!;
      t.equal(job2.id, jobHandle2.id);
      await jobHandle2.sync();
      t.same(await jobHandle2.getChildJobIds(), [jobHandle3.id]);
      t.same(jobHandle2.parentJobIds, []);
      t.notOk(await worker.getNextExecutor());
      t.ok(await job1.markSucceeded());
      t.notOk(await worker.getNextExecutor());
      t.ok(await job2.markFailed());
      t.notOk(await worker.getNextExecutor());
      t.ok(await jobHandle2.retry());
      const job3 = (await worker.getNextExecutor())!;
      t.equal(job3.id, jobHandle2.id);
      t.ok(await job3.markSucceeded());
      const job4 = (await worker.getNextExecutor())!;
      t.equal(job4.id, jobHandle3.id);
      await jobHandle3.sync();
      t.same(await jobHandle3.getChildJobIds(), []);
      t.same(jobHandle3.parentJobIds, [jobHandle1.id, jobHandle2.id]);

      t.equal((await queue.getStatistics()).succeededJobs, 2);
      await queue.prune({ jobExpungePeriod: 0 });
      t.equal((await queue.getStatistics()).succeededJobs, 0);
      t.ok(await job4.markSucceeded());
      t.equal((await queue.getStatistics()).succeededJobs, 1);
      await queue.prune({ jobExpungePeriod: 0 });
      t.equal((await queue.getStatistics()).succeededJobs, 0);

      const jobHandle4 = await queue.addJob('test', {}, { parentJobIds: [-1] });
      const job5 = (await worker.getNextExecutor())!;
      t.equal(job5.id, jobHandle4.id);
      t.ok(await job5.markSucceeded());
      const jobHandle5 = await queue.addJob('test', {}, { parentJobIds: [-1] });
      const job6 = (await worker.getNextExecutor())!;
      t.equal(job6.id, jobHandle5.id);
      await jobHandle5.sync();
      t.same(jobHandle5.parentJobIds, [-1]);
      const jobHandle5a = (await jobHandle5.retry({ parentJobIds: [-1, -2] }))!;
      const job7 = (await worker.getNextExecutor())!;
      await jobHandle5a.sync();
      t.same(jobHandle5a.parentJobIds, [-1, -2]);
      t.ok(await job7.markSucceeded());

      const jobHandle6 = await queue.addJob('test');
      const jobHandle7 = await queue.addJob('test');
      const jobHandle8 = await queue.addJob('test', {}, { parentJobIds: [jobHandle6.id, jobHandle7.id] });
      const parentIds = jobHandle8.parentJobIds;
      t.equal(parentIds.length, 2);
      t.equal(parentIds[0], jobHandle6.id);
      t.equal(parentIds[1], jobHandle7.id);
      t.ok(await jobHandle6.remove());
      t.ok(await jobHandle7.remove());
      t.ok(await jobHandle8.remove());

      await worker.unregister();
    });

    await t.test('Job dependencies (lax)', async (t) => {
      const worker = await queue.getNewWorker().register();
      const jobHandle1 = await queue.addJob('test');
      const jobHandle2 = await queue.addJob('test');
      const jobHandle3 = await queue.addJob(
        'test',
        {},
        { laxDependency: true, parentJobIds: [jobHandle1.id, jobHandle2.id] },
      );
      const job1 = (await worker.getNextExecutor())!;
      t.equal(job1.id, jobHandle1.id);
      await jobHandle1.sync();
      t.same(await jobHandle1.getChildJobIds(), [jobHandle3.id]);
      t.same(jobHandle1.parentJobIds, []);
      const job2 = (await worker.getNextExecutor())!;
      t.equal(job2.id, jobHandle2.id);
      await jobHandle2.sync();
      t.same(await jobHandle2.getChildJobIds(), [jobHandle3.id]);
      t.same(jobHandle2.parentJobIds, []);
      t.notOk(await worker.getNextExecutor());
      t.ok(await job1.markSucceeded());
      t.notOk(await worker.getNextExecutor());
      t.ok(await job2.markFailed());
      const job3 = (await worker.getNextExecutor())!;
      t.ok(job3);
      t.equal(job3.id, jobHandle3.id);
      await jobHandle3.sync();
      t.same(await jobHandle3.getChildJobIds(), []);
      t.same(jobHandle3.parentJobIds, [jobHandle1.id, jobHandle2.id]);
      t.ok(await job3.markSucceeded());

      const jobHandle4 = await queue.addJob('test');
      const jobHandle5 = await queue.addJob('test', {}, { parentJobIds: [jobHandle4.id] });
      const job4 = (await worker.getNextExecutor())!;
      t.equal(job4.id, jobHandle4.id);
      t.notOk(await worker.getNextExecutor());
      t.ok(await job4.markFailed());
      t.notOk(await worker.getNextExecutor());
      t.ok(await queue.getJob(jobHandle5.id).then((job) => job!.retry({ laxDependency: true })));
      const job5 = (await worker.getNextExecutor())!;
      t.equal(job5.id, jobHandle5.id);
      await jobHandle5.sync();
      t.same(await jobHandle5.getChildJobIds(), []);
      t.same(jobHandle5.parentJobIds, [jobHandle4.id]);
      t.ok(await job5.markSucceeded());
      t.ok(await jobHandle4.remove());

      t.same((await queue.listJobInfos({ ids: [jobHandle5.id] }).next())!.laxDependency, true);
      t.ok(await queue.getJob(jobHandle5.id).then((job) => job!.retry()));
      t.same((await queue.listJobInfos({ ids: [jobHandle5.id] }).next())!.laxDependency, true);
      t.ok(await queue.getJob(jobHandle5.id).then((job) => job!.retry({ laxDependency: false })));
      t.same((await queue.listJobInfos({ ids: [jobHandle5.id] }).next())!.laxDependency, false);
      t.ok(await queue.getJob(jobHandle5.id).then((job) => job!.retry()));
      t.same((await queue.listJobInfos({ ids: [jobHandle5.id] }).next())!.laxDependency, false);
      t.ok(await queue.getJob(jobHandle5.id).then((job) => job!.remove()));
      await worker.unregister();
    });

    await t.test('Expiring jobs', async (t) => {
      await queue.resetQueue();

      const jobHandle1 = await queue.addJob('test');
      t.notOk(jobHandle1.expiresAt);
      t.ok(await queue.getJob(jobHandle1.id).then((job) => job!.remove()));

      const jobHandle2 = await queue.addJob('test', {}, { expireIn: 300000 });
      t.same(jobHandle2.expiresAt instanceof Date, true);
      const worker = await queue.getNewWorker().register();
      const job1 = (await worker.getNextExecutor())!;
      t.equal(job1.id, jobHandle2.id);
      await jobHandle2.sync();
      const expires = jobHandle2.expiresAt;
      t.same(expires instanceof Date, true);
      t.ok(await job1.markSucceeded());
      t.ok(await jobHandle2.retry({ expireIn: 600000 }));
      await jobHandle2.sync();
      t.equal(jobHandle2.state, JobState.Pending);
      t.same(jobHandle2.expiresAt instanceof Date, true);
      t.not(jobHandle2.expiresAt!.getTime(), expires!.getTime());
      await queue.prune();
      t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 1);
      const job2 = (await worker.getNextExecutor())!;
      t.equal(job2.id, jobHandle2.id);
      t.ok(await job2.markSucceeded());

      const jobHandle3 = await queue.addJob('test', {}, { expireIn: 300000 });
      t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 1);
      await backend.dateBackJobExpiresAt(jobHandle3.id, 86400 * 1000);
      await queue.prune();
      t.notOk(await worker.getNextExecutor());
      t.equal(await queue.listJobInfos({ states: [JobState.Pending] }).numRows(), 0);

      const jobHandle4 = await queue.addJob('test', {}, { expireIn: 300000 });
      const job4 = (await worker.getNextExecutor())!;
      t.equal(job4.id, jobHandle4.id);
      t.ok(await job4.markSucceeded());
      await backend.dateBackJobExpiresAt(jobHandle4.id, 86400 * 1000);
      await queue.prune();
      await jobHandle4.sync();
      t.equal(jobHandle4.state, JobState.Succeeded);

      const jobHandle5 = await queue.addJob('test', {}, { expireIn: 300000 });
      const job5 = (await worker.getNextExecutor())!;
      t.equal(job5.id, jobHandle5.id);
      t.ok(await job5.markFailed());
      await backend.dateBackJobExpiresAt(jobHandle5.id, 86400 * 1000);
      await queue.prune();
      await jobHandle5.sync();
      t.equal(jobHandle5.state, JobState.Failed);

      const jobHandle6 = await queue.addJob('test', {}, { expireIn: 300000 });
      const job6 = (await worker.getNextExecutor())!;
      t.equal(job6.id, jobHandle6.id);
      await backend.dateBackJobExpiresAt(jobHandle6.id, 86400 * 1000);
      await queue.prune();
      await jobHandle6.sync();
      t.equal(jobHandle6.state, JobState.Running);
      t.ok(await job6.markSucceeded());

      const jobHandle7 = await queue.addJob('test', {}, { expireIn: 300000 });
      const jobHandle8 = await queue.addJob('test', {}, { expireIn: 300000, parentJobIds: [jobHandle7.id] });
      t.notOk(await worker.getNextExecutor(0, { id: jobHandle8.id }));
      await backend.dateBackJobExpiresAt(jobHandle7.id, 86400 * 1000);
      await queue.prune();
      const job8 = (await worker.getNextExecutor(0, { id: jobHandle8.id }))!;
      t.ok(await job8.markSucceeded());
      await worker.unregister();
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
      t.match(stats1.queueboneVersion, /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/);
      t.equal(stats1.backendName, backend.name);
      t.match(stats1.backendVersion, /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/);
      t.ok(stats1.backendUptime);

      const worker = await queue.getNewWorker().register();
      const stats2 = await queue.getStatistics();
      t.equal(stats2.onlineWorkers, 1);
      t.equal(stats2.idleWorkers, 1);
      t.equal(stats2.busyWorkers, 0);
      const jobHandle1 = await queue.addJob('fail');
      const stats3 = await queue.getStatistics();
      t.equal(stats3.enqueuedJobs, 1);
      t.equal(stats2.onlineWorkers, 1);
      t.equal(stats2.idleWorkers, 1);
      t.equal(stats2.busyWorkers, 0);
      const jobHandle2 = await queue.addJob('fail');
      const stats4 = await queue.getStatistics();
      t.equal(stats4.enqueuedJobs, 2);
      t.equal(stats4.pendingJobs, 2);
      t.equal(stats2.onlineWorkers, 1);
      t.equal(stats2.idleWorkers, 1);
      t.equal(stats2.busyWorkers, 0);

      const job1 = (await worker.getNextExecutor(0))!;
      t.equal(job1.id, jobHandle1.id);
      const stats5 = await queue.getStatistics();
      t.equal(stats5.pendingJobs, 1);
      t.equal(stats5.runningJobs, 1);
      t.equal(stats5.onlineWorkers, 1);
      t.equal(stats5.busyWorkers, 1);

      const jobHandle3 = await queue.addJob('fail');
      const job2 = (await worker.getNextExecutor())!;
      t.equal(job2.id, jobHandle2.id);
      const stats6 = await queue.getStatistics();
      t.equal(stats6.pendingJobs, 1);
      t.equal(stats6.runningJobs, 2);
      t.equal(stats6.busyWorkers, 1);

      t.same(await job2.markSucceeded(), true);
      t.same(await job1.markSucceeded(), true);
      t.equal((await queue.getStatistics()).succeededJobs, 2);
      const job3 = (await worker.getNextExecutor())!;
      t.equal(job3.id, jobHandle3.id);
      t.same(await job3.markFailed(), true);
      t.equal((await queue.getStatistics()).failedJobs, 1);
      t.ok(await jobHandle3.retry());
      t.equal((await queue.getStatistics()).failedJobs, 0);

      const job4 = (await worker.getNextExecutor())!;
      await job4.markSucceeded({ it: 'works' });
      await worker.unregister();
      const stats7 = await queue.getStatistics();
      t.equal(stats7.pendingJobs, 0);
      t.equal(stats7.runningJobs, 0);
      t.equal(stats7.succeededJobs, 3);
      t.equal(stats7.failedJobs, 0);
      t.equal(stats7.abandonedJobs, 0);
      t.equal(stats7.canceledJobs, 0);
      t.equal(stats7.offlineWorkers, 1);
      t.equal(stats7.onlineWorkers, 0);
      t.equal(stats7.busyWorkers, 0);
      t.equal(stats7.idleWorkers, 0);

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
      t.equal(history.daily[0].succeededJobs + history.daily[22].succeededJobs, 3);
      t.equal(history.daily[0].failedJobs + history.daily[22].failedJobs, 1);
      t.equal(history.daily[23].succeededJobs, 0);
      t.equal(history.daily[23].failedJobs, 0);
      t.ok(history.daily[0].epoch > history.daily[23].epoch);
      t.ok(history.daily[1].epoch);
      t.ok(history.daily[12].epoch);
      t.ok(history.daily[23].epoch);
    });

    await queue.stop();
  });
}
