import EventEmitter from 'events';
import {Database} from './database.js';
import {Results} from './results.js';
import {parseConfig} from './config.js';
import {throwWithContext} from './util.js';
import pg from 'pg';

const DEBUG = process.env.MOJO_PG_DEBUG === '1';

export interface PgOptions extends pg.PoolConfig {
  verboseErrors?: boolean;
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

  /**
   * Show SQL context for errors.
   */
  private verboseErrors = true;

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
    if (options.verboseErrors !== undefined) this.verboseErrors = options.verboseErrors;

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
   * Get database connection from pool.
   */
  async db(): Promise<Database> {
    const client = await this.pool.connect();
    return new Database(client, {verboseErrors: this.verboseErrors});
  }

  /**
   * Close all database connections in the pool.
   */
  async end(): Promise<void> {
    if (this.doNotEnd === false) await this.pool.end();
  }


  /**
   * Perform raw SQL query.
   * @example
   * // Simple query with placeholder
   * const results = await pg.query('SELECT * FROM users WHERE name = $1', 'Sara'});
   *
   * // Query with result type
   * const results = await db.query<User>('SELECT * FROM users');
   *
   * // Query with results as arrays
   * const results = await pg.query({text: 'SELECT * FROM users', rowMode: 'array'});
   */
  async query<T = any>(query: string | pg.QueryConfig, ...values: any[]): Promise<Results<T>> {
    if (typeof query === 'string') query = {text: query, values};
    if (DEBUG === true) process.stderr.write(`\n${query.text}\n`);

    try {
      const result = await this.pool.query(query);
      const rows = result.rows;
      return rows === undefined ? new Results(result.rowCount) : new Results(result.rowCount, ...rows);
    } catch (error) {
      if (this.verboseErrors === true) throwWithContext(error, query);
      throw error;
    }
  }

  /**
   * Get all non-system tables.
   */
  async tables(): Promise<string[]> {
    const db = await this.db();
    return await db.tables().finally(() => db.release());
  }
}
