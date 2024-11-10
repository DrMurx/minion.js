import t from 'tap';
import { JobState, type JobInfo } from '../types/job.js';
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
    const jobInfo: JobInfo = {
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
      childJobIds: [],
      laxDependency: false,

      metadata: {},

      delayUntil: new Date(),
      startedAt: new Date(),

      createdAt: new Date(),

      time: new Date(),
    };
    t.equal(defaultBackoffStrategy(jobInfo), expectedDelay);
  }

  t.end();
});
