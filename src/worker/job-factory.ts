import { type Job, type JobArgs } from '../types/job.js';
import { type JobFactory } from '../types/queue.js';
import { type Executor } from './executor.js';
import { DefaultJob } from './job.js';

export class DefaultJobFactory<BaseJob extends Job<JobArgs>> implements JobFactory<BaseJob> {
  createJobObject<ResultJob extends BaseJob>(executor: Executor<ResultJob>): ResultJob {
    return new DefaultJob(executor) as unknown as ResultJob;
  }
}
