export interface StatsReader {
  /**
   * Get history information for job queue.
   */
  getJobStatistics(): Promise<QueueJobStatistics>;

  /**
   * Get statistics for the job queue.
   */
  getStatistics(): Promise<QueueStats>;
}

export interface QueueJobStatistics {
  daily: DailyJobHistory[];
}

export interface DailyJobHistory {
  epoch: number;
  succeededJobs: number;
  failedJobs: number;
  abortedJobs: number;
  abandonedJobs: number;
  unattendedJobs: number;
}

export interface QueueStats {
  enqueuedJobs: number;
  pendingJobs: number;
  scheduledJobs: number;
  runningJobs: number;
  succeededJobs: number;
  failedJobs: number;
  abortedJobs: number;
  abandonedJobs: number;
  unattendedJobs: number;
  canceledJobs: number;

  offlineWorkers: number;
  onlineWorkers: number;
  idleWorkers: number;
  busyWorkers: number;
  lostWorkers: number;

  queueboneVersion: string;

  backendName: string;
  backendVersion: string;
  backendUptime: number;
}
