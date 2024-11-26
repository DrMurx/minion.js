import { type WorkerConfig } from '@queuebone/core';

export interface WorkerProfile {
  /**
   * Name of this worker profile.
   */
  name: string;

  /**
   * The passphrase for the worker registration.
   */
  passphrase: string;

  /**
   * Number of workers allowed to connect at the same time with this profile.
   */
  maxWorkers?: number;

  /**
   * The worker config used for this profile
   */
  config?: Partial<WorkerConfig>;
}
