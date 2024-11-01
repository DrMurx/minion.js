import t from 'tap';
import { JobState, type JobDescriptor } from './types/job.ts';
import { Executor } from './worker/executor.ts';
import { DefaultJob } from './job.ts';

t.test('Default backoff strategy', async (t) => {
  for (const [attempt, expectedDelay] of [
    [0, 15],
    [1, 16],
    [2, 31],
    [3, 96],
    [4, 271],
    [5, 640],
    [25, 390640],
  ]) {
    const jobInfo: JobDescriptor = {
      id: 0,
      taskName: '',
      args: {},
      maxAttempts: 0,
      attempt,
    };
    const executor = new Executor(jobInfo, JobState.Running, null as any, null as any);
    const job = new DefaultJob(executor);
    t.equal(await job.getBackoffDelay(), expectedDelay);
  }

  t.end();
});
