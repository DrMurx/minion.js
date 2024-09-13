import type {Connection} from './connection.js';

/**
 * PostgreSQL transaction class.
 */
export class Transaction {
  private finished = false;

  constructor(private conn: Connection) {}

  async [Symbol.asyncDispose]() {
    await this.rollback();
  }

  /**
   * Commit transaction. Does nothing if `tx.rollback()` has been called first.
   */
  async commit(): Promise<void> {
    if (this.finished === true) return;
    await this.conn.client.query('COMMIT');
    this.finished = true;
  }

  /**
   * Rollback transaction. Does nothing if `tx.commit()` has been called first.
   */
  async rollback(): Promise<void> {
    if (this.finished === true) return;
    await this.conn.client.query('ROLLBACK');
    this.finished = true;
  }
}
