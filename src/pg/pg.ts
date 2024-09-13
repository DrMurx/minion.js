import EventEmitter from 'events';
import {Connection} from './connection.js';
import {Results} from './results.js';
import {parseConfig} from './config.js';
import {throwWithContext} from './util.js';
import pg from 'pg';

export interface PgOptions extends pg.PoolConfig {
  searchPath?: string[];
}

export type PgConfig = string | pg.PoolConfig | Pg;

/**
 * PostgreSQL pool class.
 */
export class Pg extends EventEmitter {
  /**
   * PostgreSQL connection pool.
   */
  public pool: pg.Pool;

  /**
   * Search path.
   */
  private searchPath: string[] = [];

  private doNotEnd = false;

  constructor(config: PgConfig | undefined, options: PgOptions = {}) {
    super();

    if (config instanceof Pg) {
      this.pool = config.pool;
      this.doNotEnd = true;
    } else {
      this.pool = new pg.Pool({allowExitOnIdle: true, ...options, ...parseConfig(config)});
    }

    if (options.searchPath !== undefined) this.searchPath = options.searchPath;

    // Convert BIGINT to number (even if not all 64bit are usable)
    pg.types.setTypeParser(20, parseInt);

    this.pool.on('connect', client => {
      if (this.searchPath.length > 0) {
        const searchPath = this.searchPath.map(path => client.escapeIdentifier(path)).join(', ');
        client.query(`SET search_path TO ${searchPath}`);
      }
    });
  }

  async [Symbol.asyncDispose]() {
    await this.end();
  }

  /**
   * Close all database connections in the pool.
   */
  async end(): Promise<void> {
    if (this.doNotEnd === false) await this.pool.end();
  }

  /**
   * Get database connection from pool.
   */
  async getConnection(): Promise<Connection> {
    const client = await this.pool.connect();
    return new Connection(client);
  }

  /**
   * Perform raw SQL query.
   * @example
   * // Simple query with placeholder
   * const results = await pg.query('SELECT * FROM users WHERE name = $1', 'Sara'});
   *
   * // Query with result type
   * const results = await pg.query<User>('SELECT * FROM users');
   *
   * // Query with results as arrays
   * const results = await pg.query({text: 'SELECT * FROM users', rowMode: 'array'});
   */
  async query<T = any>(query: string | pg.QueryConfig, ...values: any[]): Promise<Results<T>> {
    if (typeof query === 'string') query = {text: query, values};

    try {
      const result = await this.pool.query(query);
      const rows = result.rows;
      return rows === undefined ? new Results(result.rowCount) : new Results(result.rowCount, ...rows);
    } catch (error) {
      throwWithContext(error, query);
    }
  }

  /**
   * Get all non-system tables.
   */
  async getTables(): Promise<string[]> {
    const conn = await this.getConnection();
    return await conn.getTables().finally(() => conn.release());
  }
}
