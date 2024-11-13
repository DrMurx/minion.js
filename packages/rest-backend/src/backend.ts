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
import axios, { type AxiosInstance } from 'axios';

export class RestBackend implements Backend {
  public readonly FOREGROUND_QUEUE = '_foreground_queue';
  public readonly name = 'Http';

  private _axios: AxiosInstance;

  constructor(baseUrl: string, bearerToken: string) {
    this._axios = axios.create({
      baseURL: baseUrl,
      headers: {
        Authorization: `Bearer ${bearerToken}`,
      },
    });
  }

  get axios(): AxiosInstance {
    return this._axios;
  }

  setRequeueHandler() {
    // do nothing - this is a server only operation
  }

  async addJob<Args extends JobArgs>(): Promise<JobInfo<Args>> {
    throw new Error('Unsupported function: addJob');
  }

  async retryJob<Args extends JobArgs>(): Promise<JobInfo<Args> | undefined> {
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
      const body = {
        metadata: records,
      };
      const response = await this._axios.patch<{ metadata?: Record<string, any> }>(`/jobs/${jobId}/${attempt}`, body);
      return response.data.metadata ?? {};
    } catch (_) {
      return undefined;
    }
  }

  async updateJobProgress(jobId: JobId, attempt: number, progress: number): Promise<boolean> {
    try {
      const body = {
        progress,
      };
      await this._axios.patch(`/jobs/${jobId}/${attempt}`, body);
      return true;
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
      const body = {
        state,
        result,
      };
      await this._axios.patch(`/jobs/${jobId}/${attempt}`, body);
      return true;
    } catch (_) {
      return false;
    }
  }

  async assignNextJob<Args extends JobArgs>(
    id: WorkerId,
    taskNames: string[],
    _: number,
    options: JobDequeueOptions,
  ): Promise<JobInfo<Args> | null> {
    try {
      const body = {
        taskNames,
        options: {
          minPriority: options.minPriority,
        },
      };
      const response = await this._axios.post<JobInfo<Args>>(`/workers/${id}/nextjob`, body);
      return response.data;
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
      const response = await this._axios.post<WorkerInfo>('/workers', {});
      return response.data;
    } catch (e) {
      throw new Error(`Can't register worker. ${e}`);
    }
  }

  async updateWorker(id: WorkerId, options: WorkerUpdateOptions): Promise<WorkerInfo | undefined> {
    try {
      const body: ClientWorkerUpdateOptions = {
        state: options.state,
      };
      const response = await this._axios.patch(`/workers/${id}`, body);
      return response.data;
    } catch (_) {
      return undefined;
    }
  }

  async checkWorkerInbox(id: WorkerId, options: WorkerUpdateOptions): Promise<WorkerCommandDescriptor[]> {
    try {
      const body: ClientWorkerUpdateOptions = {
        state: options.state,
      };
      const response = await this._axios.post<WorkerCommandDescriptor[]>(`/workers/${id}/inbox`, body);
      return response.data;
    } catch (_) {
      return [];
    }
  }

  async unregisterWorker(id: WorkerId): Promise<boolean> {
    try {
      await this._axios.delete(`/workers/${id}`);
      return true;
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
