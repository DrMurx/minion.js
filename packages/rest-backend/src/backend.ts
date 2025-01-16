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
import { Axios, AxiosError, AxiosResponse } from 'axios';
import { createAxios, parseConfig } from './factory.js';

export class RestBackend implements Backend {
  public readonly name = 'Http';

  private _axios: Axios;
  private _apikey: string;

  private workerTokens: Map<WorkerId, string> = new Map();
  private jobTokens: Map<JobId, string> = new Map();

  constructor(config: string | URL | Axios, apikey?: string) {
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
    } else {
      throw new ConfigurationError('Invalid config for RestBackend');
    }
  }

  get axios(): Axios {
    return this._axios;
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
        signal: AbortSignal.timeout(500),
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
      return response.status === 200 ? (response.data.metadata ?? {}) : undefined;
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
        signal: AbortSignal.timeout(500),
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
      return response.status === 200;
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
      });
      this.jobTokens.delete(jobId);
      return response.status === 200;
    } catch (_) {
      return false;
    }
  }

  async assignNextJob<Args extends JobArgs>(
    id: WorkerId,
    taskNames: string[],
    _: number,
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
      });
      if (response.status === 200) {
        this.jobTokens.set(response.data.id, token!);
        return response.data;
      }
      return null;
    } catch (_) {
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
      response = await this._axios.post('/workers', {
        apikey: this._apikey,
      });
    } catch (e) {
      if (e instanceof AxiosError && e.code === 'ECONNREFUSED') {
        throw new ConnectionError('Server refused connection', { cause: e });
      }
      throw new ConnectionError("Can't register worker", { cause: e });
    }

    if (response.status === 401) {
      throw new ConnectionError(`Unable to authenticate at server`, {
        code: 'AUTHENTICATION_FAILED',
        cause: response,
      });
    }

    if (response.status !== 200) {
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
      });
      return response.status === 200 ? response.data : undefined;
    } catch (_) {
      console.log(_);
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
      });
      return response.status === 200 ? response.data : [];
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
      return response.status === 200;
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

    if (response.status !== 200 || !response.data.status || typeof response.data.status !== 'string') {
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
