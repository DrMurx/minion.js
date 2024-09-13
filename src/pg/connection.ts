import type {Notification, PoolClient} from 'pg';
import EventEmitter, {on} from 'events';
import {Results} from './results.js';
import {throwWithContext} from './util.js';

interface ConnectionEvents {
  end: (this: Connection) => void;
  notification: (this: Connection, message: Notification) => void;
}

declare interface ConnectionEventEmitter {
  on: <T extends keyof ConnectionEvents>(event: T, listener: ConnectionEvents[T]) => this;
  once: <T extends keyof ConnectionEvents>(event: T, listener: ConnectionEvents[T]) => this;
  emit: <T extends keyof ConnectionEvents>(event: T, ...args: Parameters<ConnectionEvents[T]>) => boolean;
}

interface PidResult {
  pg_backend_pid: number;
}

interface TablesResult {
  schemaname: string;
  tablename: string;
}

/**
 * PostgreSQL database connection class.
 */
export class Connection extends EventEmitter implements ConnectionEventEmitter {
  private channels: string[] = [];

  /**
   * @param client PostgreSQL client.
   * @param options
   */
  constructor(public client: PoolClient) {
    super();
    client.on('end', () => this.emit('end'));
    client.on('notification', message => this.emit('notification', message));
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<Notification> {
    const ac = new AbortController();
    this.once('end', () => ac.abort());

    try {
      for await (const [message] of on(this, 'notification', {signal: ac.signal})) {
        yield message;
      }
    } catch (error) {
      if (!(error instanceof Error) || error.name !== 'AbortError') throw error;
    }
  }

  async [Symbol.asyncDispose]() {
    await this.release();
  }

  /**
   * Close database connection.
   */
  async end(): Promise<void> {
    await (this.client as any).end();
  }

  /**
   * Release database connection back into the pool.
   */
  async release(): Promise<void> {
    const client = this.client;
    ['end', 'notification'].forEach(event => client.removeAllListeners(event));
    if (this.channels.length > 0) await this.unlisten();
    client.release();
  }

  /**
   * Perform raw SQL query.
   * @example
   * // Simple query with placeholder
   * const results = await conn.query('SELECT * FROM users WHERE name = $1', 'Sara'});
   *
   * // Query with result type
   * const results = await conn.query<User>('SELECT * FROM users');
   */
  async query<T = any>(query: string, ...values: any[]): Promise<Results<T>> {
    try {
      const result = await this.client.query(query, values);
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
    const results = await this.query<TablesResult>(`
      SELECT schemaname, tablename FROM pg_catalog.pg_tables
      WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'`);
    return results.map(row => `${row.schemaname}.${row.tablename}`);
  }

  /**
   * Get backend process id.
   */
  async getBackendPid(): Promise<number> {
    return (await this.query<PidResult>('SELECT pg_backend_pid()'))[0].pg_backend_pid;
  }

  /**
   * Send notification.
   */
  async notify(channel: string, payload?: string): Promise<void> {
    const client = this.client;
    const escapedChannel = client.escapeIdentifier(channel);

    // No payload
    if (payload === undefined) {
      await this.client.query(`NOTIFY ${escapedChannel}`);
    }

    // Payload
    else {
      const escapedPayload = client.escapeLiteral(payload);
      await this.client.query(`NOTIFY ${escapedChannel}, ${escapedPayload}`);
    }
  }

  /**
   * Listen for notifications.
   */
  async listen(channel: string): Promise<void> {
    const client = this.client;
    const escapedChannel = client.escapeIdentifier(channel);
    await this.client.query(`LISTEN ${escapedChannel}`);
    this.channels.push(channel);
  }

  /**
   * Stop listenting for notifications.
   */
  async unlisten(channel?: string): Promise<void> {
    const client = this.client;

    // All channels
    if (channel === undefined) {
      await this.client.query('UNLISTEN *');
      this.channels = [];
    }

    // One channel
    else {
      const escapedChannel = client.escapeIdentifier(channel);
      await this.client.query(`UNLISTEN ${escapedChannel}`);
      this.channels = this.channels.filter(c => c !== channel);
    }
  }
}
