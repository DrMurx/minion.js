import {Minion} from "./minion.js";
import {ListJobsOptions, ListWorkersOptions} from "./types.js";

/**
 * Iterator object.
 */
export class BackendIterator<T> {
  /**
   * Number of results to fetch at once.
   */
  fetch = 10;
  /**
   * List options.
   */
  options: ListWorkersOptions & ListJobsOptions;

  _cache: T[] = [];
  _count = 0;
  _minion: Minion;
  _name: string;
  _total = 0;

  constructor(minion: Minion, name: string, options: ListWorkersOptions & ListJobsOptions) {
    this.options = options;

    this._minion = minion;
    this._name = name;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const backendIterator = this;
    return {
      async next(): Promise<IteratorResult<T>> {
        const value = (await backendIterator.next()) as any;
        return {value, done: value === undefined};
      }
    };
  }

  /**
   * Get next result.
   */
  async next(): Promise<T | undefined> {
    const cache = this._cache;
    if (cache.length < 1) await this._fetch();
    return cache.shift();
  }

  /**
   * Total number of results.
   */
  async total(): Promise<number> {
    if (this._total === 0) await this._fetch();
    return this._total;
  }

  async _fetch(): Promise<void> {
    const name = this._name;
    const methodName = name === 'workers' ? 'listWorkers' : 'listJobs';
    const results = (await this._minion.backend[methodName](0, this.fetch, this.options)) as any;
    const batch = results[name];

    const len = batch.length;
    if (len > 0) {
      this._total = results.total + this._count;
      this._count += len;
      this._cache.push(...batch);
      this.options.before = batch[batch.length - 1].id;
    }
  }
}
