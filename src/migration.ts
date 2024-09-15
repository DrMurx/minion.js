import pg from 'pg';

export interface MigrationStep {
  version: number;
  sql: string;
}

interface VersionResult {
  version: number;
}

/**
 * PostgreSQL migration class.
 */
export class Migration {
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
      const results = await this.conn.query<VersionResult>('SELECT version FROM mojo_migrations WHERE name = $1', [this._name]);
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
   * Migrate from `active` to a different version, up or down, defaults to using the `latest` version. All version
   * numbers need to be positive, with version `0` representing an empty database.
   */
  async migrate(): Promise<void> {
    const latest = this.latest;

    // Make sure migration table exists
    await this.conn.query(`
      CREATE TABLE IF NOT EXISTS mojo_migrations (
        name    TEXT PRIMARY KEY,
        version BIGINT NOT NULL CHECK (version >= 0)
      )
    `);

    await this.conn.query('BEGIN');
    try {
      // Lock migrations table and check version again
      await this.conn.query('LOCK TABLE mojo_migrations IN EXCLUSIVE MODE');

      // Check versions
      const active = await this.currentVersion();
      if (active === latest) return;
      if (active > latest) throw new Error(`Active version ${active} is greater than the latest version ${latest}`);

      const steps = this.steps
        .filter(step => step.version > active && step.version <= latest)
        .toSorted((a, b) => a.version - b.version);
      for (const step of steps) {
        await this.conn.query(step.sql);
        await this.conn.query(`
          INSERT INTO mojo_migrations (name, version) VALUES ($1, $2)
          ON CONFLICT (name) DO UPDATE SET version = $2;
        `, [this._name, step.version]);
        }
      await this.conn.query('COMMIT');
    } finally {
      await this.conn.query('ROLLBACK');
    }
  }
}
