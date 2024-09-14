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
    this.client.removeAllListeners('end').removeAllListeners('notification');
    if (this.channels.length > 0) await this.unlisten();
    this.client.release();
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
   * Get backend process id.
   */
  async getBackendPid(): Promise<number> {
    return (await this.query<PidResult>('SELECT pg_backend_pid()'))[0].pg_backend_pid;
  }

  /**
   * Send notification.
   */
  async notify(channel: string, payload?: string): Promise<void> {
    const escapedChannel = this.client.escapeIdentifier(channel);
    const escapedPayload = payload !== undefined ? `, ${this.client.escapeLiteral(payload)}` : '';
    await this.client.query(`NOTIFY ${escapedChannel}${escapedPayload}`);
  }

  /**
   * Listen for notifications.
   */
  async listen(channel: string): Promise<void> {
    const escapedChannel = this.client.escapeIdentifier(channel);
    await this.client.query(`LISTEN ${escapedChannel}`);
    this.channels.push(channel);
  }

  /**
   * Stop listenting for notifications.
   */
  async unlisten(channel?: string): Promise<void> {
    const allChannels = channel === undefined;
    const client = this.client;
    const escapedChannel = allChannels ? '*' : client.escapeIdentifier(channel);
    await this.client.query(`UNLISTEN ${escapedChannel}`);
    this.channels = allChannels ? [] : this.channels.filter(c => c !== channel);
  }
}
