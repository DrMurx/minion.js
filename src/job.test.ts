import t from 'tap';
import { DefaultJob } from './job.ts';
import { type JobDescriptor } from './types/job.ts';

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
    const jobDescriptor: JobDescriptor = {
      id: 0,
      taskName: '',
      args: {},
      maxAttempts: 0,
      attempt,
    };
    const job = new DefaultJob(null as any, jobDescriptor);
    t.equal(await job.getBackoffDelay(), expectedDelay);
  }

  t.end();
});
