import {readdir, readFile} from 'fs/promises';
import { join } from 'path';
import pg, { ClientBase } from 'pg';

interface MigrationOptions {
  name?: string;
}

interface Step {
  direction: 'down' | 'up';
  sql: string;
  version: number;
}

interface VersionResult {
  version: number;
}

type Steps = Step[];

/**
 * PostgreSQL migrations class.
 */
export class Migrations {
  /**
   * Name for this set of migrations.
   */
  public name = 'migrations';

  private steps: Steps = [];

  constructor(private conn: ClientBase) {}

  /**
   * Currently active version.
   */
  async currentVersion(): Promise<number> {
    try {
      const results = await this.conn.query<VersionResult>('SELECT version FROM mojo_migrations WHERE name = $1', [this.name]);
      const first = results.rows[0];
      return first === undefined ? 0 : first.version;
    } catch (error: any) {
      if (error.code !== '42P01') throw error;
    }
    return 0;
  }

  /**
   * Extract migrations from a directory.
   */
  async loadFromDirectory(dir: string, options: MigrationOptions = {}): Promise<void> {
    if (options.name !== undefined) this.name = options.name;

    const steps: Steps = [];
    const dirents = (await readdir(dir, {recursive: true, withFileTypes: true})).filter(d => d.isFile())
    for (const dirent of dirents) {
      const file = join(dirent.path, dirent.name);
      const dirMatch = dirent.path.substring(dir.length).match(/^[\\/](\d+)[\\/]*/);
      const fileMatch = dirent.name.match(/^(up|down)\.sql$/);
      if (dirMatch === null || fileMatch === null) continue;

      steps.push({
        direction: fileMatch[1] === 'up' ? 'up' : 'down',
        sql: (await readFile(file, 'utf8')).toString(),
        version: parseInt(dirMatch[1])
      });
    }

    this.steps = steps;
  }

  /**
   * Extract migrations from a file.
   */
  async loadFromFile(file: string, options?: MigrationOptions): Promise<void> {
    this.loadFromString((await readFile(file, {encoding: 'utf8'})), options);
  }

  /**
   * Extract migrations from string.
   */
  loadFromString(str: string, options: MigrationOptions = {}): void {
    if (options.name !== undefined) this.name = options.name;

    const steps: Steps = [];
    for (const line of str.split('\n')) {
      const match = line.match(/^\s*--\s*(\d+)\s*(up|down)/i);

      // Version line
      if (match !== null) {
        steps.push({direction: match[2].toLowerCase() === 'up' ? 'up' : 'down', sql: '', version: parseInt(match[1])});
      }

      // SQL
      else if (steps.length > 0) {
        steps[steps.length - 1].sql += line + '\n';
      }
    }

    this.steps = steps;
  }

  /**
   * Latest version.
   */
  get latest(): number {
    return this.steps.filter(step => step.direction === 'up').sort((a, b) => b.version - a.version)[0]?.version ?? 0;
  }

  /**
   * Migrate from `active` to a different version, up or down, defaults to using the `latest` version. All version
   * numbers need to be positive, with version `0` representing an empty database.
   */
  async migrateTo(target?: number): Promise<void> {
    const latest = this.latest;
    if (target === undefined) target = latest;
    const hasStep = this.steps.find(step => step.direction === 'up' && step.version === target) !== undefined;
    if (target !== 0 && hasStep === false) throw new Error(`Version ${target} has no migration`);

    // Already the right version
    if ((await this.currentVersion()) === target) return;
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
      const active = await this.currentVersion();
      if (active === target) return;

      // Newer version
      if (active > latest) throw new Error(`Active version ${active} is greater than the latest version ${latest}`);

      const sql = this.sqlFor(active, target);
      const name = pg.escapeLiteral(this.name);
      const migration = `
        ${sql}
        INSERT INTO mojo_migrations (name, version) VALUES (${name}, ${target})
        ON CONFLICT (name) DO UPDATE SET version = ${target};
      `;
      await this.conn.query(migration);
      await this.conn.query('COMMIT');
    } finally {
      await this.conn.query('ROLLBACK');
    }
  }

  /**
   * Get SQL to migrate from one version to another, up or down.
   */
  sqlFor(from: number, to: number): string {
    // Up
    if (from < to) {
      return this.steps
        .filter(step => step.direction === 'up' && step.version > from && step.version <= to)
        .sort((a, b) => a.version - b.version)
        .map(step => step.sql)
        .join('');
    }

    // Down
    else {
      return this.steps
        .filter(step => step.direction === 'down' && step.version <= from && step.version > to)
        .sort((a, b) => b.version - a.version)
        .map(step => step.sql)
        .join('');
    }
  }
}
