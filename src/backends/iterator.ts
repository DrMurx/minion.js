import { type Backend } from '../types/backend.js';
import { type ListJobsOptions } from '../types/job.js';
import { type ListWorkersOptions } from '../types/worker.js';

export interface IteratorOptions {
  /**
   * Number of results to fetch at once.
   */
  chunkSize: number;
}

type BackendIteratorType = 'jobs' | 'workers';

/**
 * Iterator object.
 */
export class BackendIterator<T> {
  private options: ListJobsOptions | ListWorkersOptions;
  private count = 0;
  private total?: number;
  private cache: T[] = [];

  constructor(
    private backend: Backend,
    private name: BackendIteratorType,
    options: ListJobsOptions | ListWorkersOptions,
    private iteratorOptions: IteratorOptions,
  ) {
    this.options = { ...options };
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async (): Promise<IteratorResult<T>> => {
        const value = (await this.next()) as any;
        return { value, done: value === undefined };
      },
    };
  }

  /**
   * Get next result.
   */
  async next(): Promise<T | undefined> {
    const cache = this.cache;
    if (cache.length < 1) await this.loadNext();
    return cache.shift();
  }

  /**
   * Total number of results.
   */
  async numRows(): Promise<number> {
    if (this.total === undefined) await this.loadNext();
    return this.total!;
  }

  async getAll(): Promise<T[]> {
    const results: T[] = [];
    for await (const result of this) {
      results.push(result);
    }
    return results;
  }

  get highestId(): number | undefined {
    return this.options.afterId;
  }

  private async loadNext(): Promise<void> {
    const results: any =
      this.name === 'workers'
        ? await this.backend.getWorkerInfos(0, this.iteratorOptions.chunkSize, this.options)
        : await this.backend.getJobInfos(0, this.iteratorOptions.chunkSize, this.options);
    const batch = results[this.name];

    if (batch.length > 0) {
      this.total = this.count + results.total;
      this.count += batch.length;
      this.cache.push(...batch);
      this.options.afterId = batch[batch.length - 1].id;
    } else if (this.total === undefined) {
      this.total = 0;
    }
  }
}
