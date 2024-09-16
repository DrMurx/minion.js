import pg from 'pg';

export interface MigrationStep {
  version: number;
  sql: string;
}

interface VersionResult {
  version: number;
}

export const migrationTableName = 'queue_migrations';

/**
 * PostgreSQL migration class.
 */
export class Migration {
  private readonly tableName = migrationTableName;

  private _name: string;
  private steps: MigrationStep[]

  constructor(name: string, steps: MigrationStep[], private conn: pg.ClientBase) {
    this._name = name;
    this.steps = steps.toSorted((a, b) => a.version - b.version);
  }

  get name(): string {
    return this._name;
  }

  /**
   * Currently active version.
   */
  async currentVersion(): Promise<number> {
    try {
      const results = await this.conn.query<VersionResult>(`SELECT version FROM ${this.tableName} WHERE name = $1`, [this._name]);
      const first = results.rows[0];
      return first === undefined ? 0 : first.version;
    } catch (error: any) {
      if (error.code !== '42P01') throw error;
    }
    return 0;
  }

  /**
   * Latest version of the migration.
   */
  get latest(): number {
    return this.steps.at(-1)?.version ?? 0;
  }

  /**
   * Migrate from current to the latest version.
   */
  async migrate(): Promise<void> {
    // Cheap check to bail our early
    if (await this.currentVersion() === this.latest) return;

    // Make sure migration table exists
    await this.conn.query(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        name    TEXT PRIMARY KEY,
        version BIGINT NOT NULL CHECK (version >= 0)
      )
    `);

    let transactionCommitted = false;
    try {
      await this.conn.query('BEGIN');

      // Lock migrations table
      await this.conn.query(`LOCK TABLE ${this.tableName} IN EXCLUSIVE MODE`);

      // Check versions again (now reliably because we're locked)
      const latest = this.latest;
      const current = await this.currentVersion();
      if (current === latest) return;
      if (current > latest) throw new Error(`Current version ${current} is greater than the latest knowm migration version ${latest}`);

      const steps = this.steps
        .filter(step => step.version > current && step.version <= latest)
        .toSorted((a, b) => a.version - b.version);
      for (const step of steps) {
        await this.conn.query(step.sql);
        await this.conn.query(`
          INSERT INTO ${this.tableName} (name, version) VALUES ($1, $2)
          ON CONFLICT (name) DO UPDATE SET version = $2;
        `, [this._name, step.version]);
        }
      await this.conn.query('COMMIT');
      transactionCommitted = true;
    } finally {
      if (!transactionCommitted) await this.conn.query('ROLLBACK');
    }
  }
}
