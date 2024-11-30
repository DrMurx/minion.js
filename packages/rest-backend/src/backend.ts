import {
  JobState,
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
import { Axios, type AxiosBasicCredentials } from 'axios';
import { createAxios, parseConfig } from './factory.js';

export class RestBackend implements Backend {
  public readonly name = 'Http';

  private _axios: Axios;
  private _auth: AxiosBasicCredentials;

  private workerTokens: Map<WorkerId, string> = new Map();
  private jobTokens: Map<JobId, string> = new Map();

  constructor(config: string | URL | Axios, auth?: AxiosBasicCredentials) {
    if (config instanceof Axios) {
      if (auth === undefined) {
        throw new Error('Missing authentication');
      }
      this._axios = config;
      this._auth = auth;
    } else if (typeof config === 'string' || config instanceof URL) {
      const url = parseConfig(config);
      this._axios = createAxios(url);
      this._auth = auth ?? {
        username: url.username,
        password: url.password,
      };
    } else {
      throw new Error('Invalid config for PgBackend');
    }
  }

  get axios(): Axios {
    return this._axios;
  }

  setRequeueHandler() {
    // do nothing - this is a server only operation
  }

  async addJob<Args extends JobArgs>(): Promise<JobRecord<Args>> {
    throw new Error('Unsupported function: addJob');
  }

  async retryJob<Args extends JobArgs>(): Promise<JobRecord<Args> | undefined> {
    // do nothing - this is a server only operation
    return undefined;
  }

  async cancelJob(): Promise<boolean> {
    throw new Error('Unsupported function: cancelJob');
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
    throw new Error('Unsupported function: removeJob');
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
    throw new Error('Unsupported function: getJobInfo');
  }

  async getJobInfos<Args extends JobArgs>(): Promise<JobInfoList<Args>> {
    throw new Error('Unsupported function: getJobInfos');
  }

  async registerWorker(): Promise<WorkerInfo> {
    try {
      const response = await this._axios.post<{ token: string; info: WorkerInfo }>('/workers', {
        name: this._auth.username,
        passphrase: this._auth.password,
      });
      if (response.status === 200) {
        const { token, info } = response.data;
        this.workerTokens.set(info.id, token);
        return info;
      }
      throw new Error("Can't register worker");
    } catch (e) {
      throw new Error(`Can't register worker. ${e}`);
    }
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
    throw new Error('Unsupported function: getWorkerInfo');
  }

  async getWorkerInfos(): Promise<WorkerInfoList> {
    throw new Error('Unsupported function: getWorkerInfos');
  }

  async sendWorkerCommand(): Promise<boolean> {
    throw new Error('Unsupported function: sendWorkerCommand');
  }

  async getJobHistory(): Promise<QueueJobStatistics> {
    throw new Error('Unsupported function: getJobHistory');
  }

  async getStats(): Promise<QueueStats> {
    throw new Error('Unsupported function: getStats');
  }

  async updateSchema(): Promise<void> {
    // do nothing
  }

  async reset(): Promise<void> {
    throw new Error('Unsupported function: reset');
  }

  async end(): Promise<void> {
    // do nothing
  }
}

export type ClientWorkerUpdateOptions = {
  state: WorkerState;
};
