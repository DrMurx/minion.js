import { AxiosError } from 'axios';

/**
 * Configuration for the request timeouts, the number of attempts and the delay between the attempts
 */
export interface RestBackendTimeouts {
  registerWorkerTimeout: number;
  registerWorkerRetries: number;
  registerWorkerRetryDelay: (retryCount: number, error: AxiosError) => number;
  updateWorkerTimeout: number;
  updateWorkerRetries: number;
  updateWorkerRetryDelay: (retryCount: number, error: AxiosError) => number;
  amendJobTimeout: number;
  amendJobRetries: number;
  amendJobRetryDelay: (retryCount: number, error: AxiosError) => number;
  markJobFinishedTimeout: number;
  markJobFinishedRetries: number;
  markJobFinishedRetryDelay: (retryCount: number, error: AxiosError) => number;
}

export interface RestBackendOptions extends Partial<RestBackendTimeouts> {
  apikey?: string;
}
