import { type JobArgs, type JobBackoffStrategy, type JobInfo, JobState } from '../types/job.js';

export const defaultBackoffStrategy: JobBackoffStrategy = <Args extends JobArgs>(jobInfo: JobInfo<Args>) => {
  if (jobInfo.state === JobState.Abandoned) return 0;
  return jobInfo.attempt ** 4 + 15;
};
