import t from 'tap';
import { JobState, type JobRecord } from '../types/job.js';
import { defaultBackoffStrategy } from './backoff-strategy.js';

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
    const jobRecord: JobRecord = {
      id: 0,
      queueName: '',
      taskName: '',
      args: {},

      state: JobState.Failed,
      priority: 0,
      progress: 0.0,
      maxAttempts: 0,
      attempt: attempt,

      parentJobIds: [],
      laxDependency: false,

      metadata: {},

      delayUntil: new Date(),
      startedAt: new Date(),

      createdAt: new Date(),
    };
    t.equal(defaultBackoffStrategy(jobRecord), expectedDelay);
  }

  t.end();
});
