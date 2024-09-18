import t from 'tap';
import { DefaultJob } from './job.ts';
import { type JobDescriptor } from './types/job.ts';

t.test('Default backoff strategy', async (t) => {
  const jobDescriptor: JobDescriptor = {
    id: 0,
    taskName: '',
    args: {},
    maxAttempts: 0,
    attempt: 0,
  };
  const job = new DefaultJob(null as any, null as any, null as any, jobDescriptor);
  jobDescriptor.attempt = 0;
  t.equal(await job.getBackoffDelay(), 15);
  jobDescriptor.attempt = 1;
  t.equal(await job.getBackoffDelay(), 16);
  jobDescriptor.attempt = 2;
  t.equal(await job.getBackoffDelay(), 31);
  jobDescriptor.attempt = 3;
  t.equal(await job.getBackoffDelay(), 96);
  jobDescriptor.attempt = 4;
  t.equal(await job.getBackoffDelay(), 271);
  jobDescriptor.attempt = 5;
  t.equal(await job.getBackoffDelay(), 640);
  jobDescriptor.attempt = 25;
  t.equal(await job.getBackoffDelay(), 390640);

  t.end();
});
