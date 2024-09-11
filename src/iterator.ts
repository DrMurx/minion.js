import {Minion} from "./minion.js";
import {ListJobsOptions, ListWorkersOptions} from "./types.js";

/**
 * Iterator object.
 */
export class BackendIterator<T> {
  /**
   * Number of results to fetch at once.
   */
  public fetch = 10;

  private cache: T[] = [];
  private count = 0;
  private _total = 0;

  /**
   *
   * @param minion
   * @param name
   * @param options List options.
   */
  constructor(private minion: Minion, private name: string, public options: ListWorkersOptions & ListJobsOptions) {}

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
    const cache = this.cache;
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

  private async _fetch(): Promise<void> {
    const name = this.name;
    const methodName = name === 'workers' ? 'listWorkers' : 'listJobs';
    const results = (await this.minion.backend[methodName](0, this.fetch, this.options)) as any;
    const batch = results[name];

    const len = batch.length;
    if (len > 0) {
      this._total = results.total + this.count;
      this.count += len;
      this.cache.push(...batch);
      this.options.before = batch[batch.length - 1].id;
    }
  }
}
