import EventEmitter from 'events';
import {Connection} from './connection.js';
import {Results} from './results.js';
import {throwWithContext} from './util.js';
import pg from 'pg';

/**
 * PostgreSQL pool class.
 */
export class Pg extends EventEmitter {
  /**
   * PostgreSQL connection pool.
   */
  public pool: pg.Pool;

  constructor(config: string) {
    super();
    this.pool = new pg.Pool({allowExitOnIdle: true, ...Pg.parseConfig(config)});
    // Convert BIGINT to number (even if not all 64bit are usable)
    pg.types.setTypeParser(20, parseInt);
  }

  /**
   * Parse PostgreSQL connection URI.
   */
  static parseConfig(config: string): pg.PoolConfig {
    const url = new URL(config);
    if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:') {
      throw new TypeError(`Invalid URL: ${config}`);
    }

    const poolConfig: pg.PoolConfig = {};
    if (url.hostname !== '') {
      poolConfig.host = decodeURIComponent(url.hostname);
    }
    if (url.port !== '') {
      poolConfig.port = parseInt(url.port);
    }
    if (url.username !== '') {
      poolConfig.user = url.username;
    }
    if (url.password !== '') {
      poolConfig.password = url.password;
    }
    if (url.pathname.startsWith('/')) {
      poolConfig.database = decodeURIComponent(url.pathname.slice(1));
    }
    const currentSchema = url.searchParams.get('currentSchema');
    if (currentSchema !== null) {
      poolConfig.options = `-c search_path=${currentSchema}`;
    }

    return poolConfig;
  }

  async [Symbol.asyncDispose]() {
    await this.end();
  }

  /**
   * Close all database connections in the pool.
   */
  async end(): Promise<void> {
    await this.pool.end();
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
   */
  async query<T = any>(query: string, ...values: any[]): Promise<Results<T>> {
    try {
      const result = await this.pool.query(query, values);
      const rows = result.rows;
      return rows === undefined ? new Results(result.rowCount) : new Results(result.rowCount, ...rows);
    } catch (error) {
      throwWithContext(error, query);
    }
  }
}
