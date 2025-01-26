import {
  ConfigurationError,
  ConnectionError,
  JobState,
  UnsupportedOperationError,
  WorkerState,
  type Backend,
  type JobArgs,
  type JobDequeueOptions,
  type JobId,
  type JobInfo,
  type JobInfoList,
  type JobPruneResult,
  type JobRecord,
  type JobResult,
  type QueueJobStatistics,
  type QueueStats,
  type WorkerCommandDescriptor,
  type WorkerId,
  type WorkerInfo,
  type WorkerInfoList,
  type WorkerPruneResult,
  type WorkerUpdateOptions,
} from '@queuebone/core';
import axios, {
  Axios,
  AxiosError,
  HttpStatusCode,
  type AxiosInstance,
  type AxiosRequestConfig,
  type AxiosResponse,
} from 'axios';
import axiosRetry, { isRetryableError } from 'axios-retry';
import { createAxios, parseConfig } from './factory.js';
import { type RestBackendOptions, type RestBackendTimeouts } from './types.js';

export class RestBackend implements Backend {
  public static TIMEOUTS: RestBackendTimeouts = {
    registerWorkerTimeout: 3000,
    registerWorkerRetries: 5,
    registerWorkerRetryDelay: (retryCount: number) => 500 * 2 ** retryCount,
    updateWorkerTimeout: 500,
    updateWorkerRetries: 3,
    updateWorkerRetryDelay: () => 500,
    amendJobTimeout: 500,
    amendJobRetries: 1,
    amendJobRetryDelay: () => 500,
    markJobFinishedTimeout: 500,
    markJobFinishedRetries: 3,
    markJobFinishedRetryDelay: (retryCount: number) => 250 * 2 ** retryCount,
  };
  public readonly name = 'Http';

  private _axios: AxiosInstance;
  private _apikey: string;
  private _timeouts: RestBackendTimeouts;

  private workerTokens: Map<WorkerId, string> = new Map();
  private jobTokens: Map<JobId, string> = new Map();

  constructor(config: string | URL | AxiosRequestConfig | AxiosInstance, options: RestBackendOptions = {}) {
    const { apikey } = options;
    if (config instanceof Axios) {
      this._axios = config;
      if (apikey === undefined) {
        throw new ConfigurationError('Missing authentication');
      }
      this._apikey = apikey;
    } else if (typeof config === 'string' || config instanceof URL) {
      const url = parseConfig(config);
      this._axios = createAxios(url);
      if (apikey === undefined && url.password === '') {
        throw new ConfigurationError('Missing authentication');
      }
      this._apikey = apikey ?? url.password;
    } else if (config.baseURL) {
      this._axios = axios.create(config);
      if (apikey === undefined) {
        throw new ConfigurationError('Missing authentication');
      }
      this._apikey = apikey;
    } else {
      throw new ConfigurationError('Invalid config for RestBackend');
    }
    axiosRetry(this._axios);
    this._timeouts = { ...RestBackend.TIMEOUTS, ...options };
  }

  setRequeueHandler() {
    // do nothing - this is a server only operation
  }

  async addJob<Args extends JobArgs>(): Promise<JobRecord<Args>> {
    throw new UnsupportedOperationError('Unsupported function: addJob');
  }

  async retryJob<Args extends JobArgs>(): Promise<JobRecord<Args> | undefined> {
    // do nothing - this is a server only operation
    return undefined;
  }

  async cancelJob(): Promise<boolean> {
    throw new UnsupportedOperationError('Unsupported function: cancelJob');
  }

  async amendJobMetadata(
    jobId: JobId,
    attempt: number,
    records: Record<string, any>,
  ): Promise<Record<string, any> | undefined> {
    try {
      const token = this.jobTokens.get(jobId);
      const body = {
        metadata: records,
      };
      const response = await this._axios.patch<{ metadata?: Record<string, any> }>(`/jobs/${jobId}/${attempt}`, body, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
        'axios-retry': {
          retries: this._timeouts.amendJobRetries,
          retryDelay: this._timeouts.amendJobRetryDelay,
          shouldResetTimeout: true,
        },
        timeout: this._timeouts.amendJobTimeout,
      });
      return response.status === HttpStatusCode.Ok ? (response.data.metadata ?? {}) : undefined;
    } catch (_) {
      return undefined;
    }
  }

  async updateJobProgress(jobId: JobId, attempt: number, progress: number): Promise<boolean> {
    try {
      const token = this.jobTokens.get(jobId);
      const body = {
        progress,
      };
      const response = await this._axios.patch(`/jobs/${jobId}/${attempt}`, body, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
        'axios-retry': {
          retries: 0, // This might be a frequent call, so we don't retry
        },
        signal: AbortSignal.timeout(this._timeouts.amendJobTimeout),
      });
      return response.status === HttpStatusCode.Ok;
    } catch (_) {
      return false;
    }
  }

  async markJobFinished(
    jobId: JobId,
    attempt: number,
    state: JobState.Succeeded | JobState.Failed,
    result: JobResult,
  ): Promise<boolean> {
    try {
      const token = this.jobTokens.get(jobId);
      const body = {
        state,
        result,
      };
      const response = await this._axios.patch(`/jobs/${jobId}/${attempt}`, body, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
        'axios-retry': {
          retries: this._timeouts.markJobFinishedRetries,
          retryDelay: this._timeouts.markJobFinishedRetryDelay,
          retryCondition: (error) => isRetryableError(error),
          shouldResetTimeout: true,
        },
        timeout: this._timeouts.markJobFinishedTimeout,
      });
      this.jobTokens.delete(jobId);
      return response.status === HttpStatusCode.Ok;
    } catch (e: any) {
      if ('code' in e && e.code === 'ECONNREFUSED') throw e;
      return false;
    }
  }

  async assignNextJob<Args extends JobArgs>(
    id: WorkerId,
    taskNames: string[],
    timeout: number,
    options: JobDequeueOptions,
  ): Promise<JobRecord<Args> | null> {
    const token = this.workerTokens.get(id);
    try {
      const body = {
        taskNames,
        options: {
          minPriority: options.minPriority,
        },
      };
      const response = await this._axios.post<JobRecord<Args>>('/worker/nextjob', body, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
        'axios-retry': {
          retries: 0,
        },
      });
      if (response.status === HttpStatusCode.Ok) {
        this.jobTokens.set(response.data.id, token!);
        return response.data;
      }
      return null;
    } catch (e: any) {
      console.log(`backend error ${e.code} ${e.message}`);
      return null;
    }
  }

  async removeJob(): Promise<boolean> {
    throw new UnsupportedOperationError('Unsupported function: removeJob');
  }

  async pruneJobs<Args extends JobArgs>(): Promise<JobPruneResult<Args>> {
    return {
      expiredJobs: [],
      abandonedJobs: [],
      unattendedJobs: [],
      expungedJobs: [],
    };
  }

  async getJobInfo<Args extends JobArgs>(): Promise<JobInfo<Args> | undefined> {
    throw new UnsupportedOperationError('Unsupported function: getJobInfo');
  }

  async getJobInfos<Args extends JobArgs>(): Promise<JobInfoList<Args>> {
    throw new UnsupportedOperationError('Unsupported function: getJobInfos');
  }

  async registerWorker(): Promise<WorkerInfo> {
    let response: AxiosResponse<{ token: string; info: WorkerInfo }>;
    try {
      const body = {
        apikey: this._apikey,
      };
      response = await this._axios.post('/workers', body, {
        'axios-retry': {
          retries: this._timeouts.registerWorkerRetries,
          retryDelay: this._timeouts.registerWorkerRetryDelay,
          retryCondition: (error) => isRetryableError(error),
          shouldResetTimeout: true,
        },
        timeout: this._timeouts.registerWorkerTimeout,
      });
    } catch (e: any) {
      if ('code' in e && e.code === 'ECONNREFUSED') {
        throw new ConnectionError('Server refused connection', { cause: e });
      }
      throw new ConnectionError("Can't register worker", { cause: e });
    }

    if (response.status === HttpStatusCode.Unauthorized) {
      throw new ConnectionError(`Unable to authenticate at server`, {
        code: 'AUTHENTICATION_FAILED',
        cause: response,
      });
    }

    if (response.status !== HttpStatusCode.Ok) {
      throw new ConnectionError("Can't register worker", { cause: response });
    }

    const { token, info } = response.data;
    this.workerTokens.set(info.id, token);
    return info;
  }

  async updateWorker(id: WorkerId, options: WorkerUpdateOptions): Promise<WorkerInfo | undefined> {
    try {
      const token = this.workerTokens.get(id);
      const body: ClientWorkerUpdateOptions = {
        state: options.state,
      };
      const response = await this._axios.patch('/worker', body, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
        'axios-retry': {
          retries: this._timeouts.updateWorkerRetries,
          retryDelay: this._timeouts.updateWorkerRetryDelay,
          shouldResetTimeout: true,
        },
        timeout: this._timeouts.updateWorkerTimeout,
      });
      return response.status === HttpStatusCode.Ok ? response.data : undefined;
    } catch (_) {
      return undefined;
    }
  }

  async checkWorkerInbox(id: WorkerId, options: WorkerUpdateOptions): Promise<WorkerCommandDescriptor[]> {
    try {
      const token = this.workerTokens.get(id);
      const body: ClientWorkerUpdateOptions = {
        state: options.state,
      };
      const response = await this._axios.post<WorkerCommandDescriptor[]>('/worker/inbox', body, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
        'axios-retry': {
          retries: this._timeouts.updateWorkerRetries,
          retryDelay: this._timeouts.updateWorkerRetryDelay,
          shouldResetTimeout: true,
        },
        timeout: this._timeouts.updateWorkerTimeout,
      });
      return response.status === HttpStatusCode.Ok ? response.data : [];
    } catch (_) {
      return [];
    }
  }

  async unregisterWorker(id: WorkerId): Promise<boolean> {
    try {
      const token = this.workerTokens.get(id);
      const response = await this._axios.delete('/worker', {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
      this.workerTokens.delete(id);
      return response.status === HttpStatusCode.Ok;
    } catch (_) {
      return false;
    }
  }

  async pruneWorkers(): Promise<WorkerPruneResult> {
    return {
      lostWorkers: [],
    };
  }

  async getWorkerInfo(): Promise<WorkerInfo | undefined> {
    throw new UnsupportedOperationError('Unsupported function: getWorkerInfo');
  }

  async getWorkerInfos(): Promise<WorkerInfoList> {
    throw new UnsupportedOperationError('Unsupported function: getWorkerInfos');
  }

  async sendWorkerCommand(): Promise<boolean> {
    throw new UnsupportedOperationError('Unsupported function: sendWorkerCommand');
  }

  async getJobHistory(): Promise<QueueJobStatistics> {
    throw new UnsupportedOperationError('Unsupported function: getJobHistory');
  }

  async getStats(): Promise<QueueStats> {
    throw new UnsupportedOperationError('Unsupported function: getStats');
  }

  async start(): Promise<void> {
    // Do an initial ping to the backend
    let response: AxiosResponse<{ status: 'pong' | 'authenticated' }>;
    try {
      response = await this._axios.post('/ping', {
        apikey: this._apikey,
      });
    } catch (e) {
      if (e instanceof AxiosError && e.code === 'ECONNREFUSED') {
        throw new ConnectionError('Server refused connection', { cause: e });
      }
      throw new ConnectionError("Can't ping server", { cause: e });
    }

    if (response.status !== HttpStatusCode.Ok || !response.data.status || typeof response.data.status !== 'string') {
      throw new ConnectionError('Malformed response while pinging server', { cause: response });
    }

    const { status } = response.data;
    if (status !== 'authenticated') {
      throw new ConnectionError('Unable to authenticate at server', {
        code: 'AUTHENTICATION_FAILED',
        cause: response,
      });
    }
  }

  async end(): Promise<void> {
    // do nothing
  }

  async reset(): Promise<void> {
    throw new UnsupportedOperationError('Unsupported function: reset');
  }
}

export type ClientWorkerUpdateOptions = {
  state?: WorkerState;
};
