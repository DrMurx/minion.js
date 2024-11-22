import { type JobArgs, type JobBackoffStrategy, type JobRecord, JobState } from '../types/job.js';

export const defaultBackoffStrategy: JobBackoffStrategy = <Args extends JobArgs>(jobRecord: JobRecord<Args>) => {
  if (jobRecord.state === JobState.Abandoned) return 0;
  return jobRecord.attempt ** 4 + 15;
};
