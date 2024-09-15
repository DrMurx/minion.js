import { PoolClient } from 'pg';
import t from 'tap';
import { PgBackend } from './pg-backend.js';
import { Migration, MigrationStep } from './migration.js';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};
const pgConfig = process.env.TEST_ONLINE!;

t.test('Migrations', skip, async t => {
  // Isolate tests
  const pool = PgBackend.connect(`${pgConfig}?currentSchema=mojo_migrations_test`);
  const conn = await pool.connect();
  await pool.query('DROP SCHEMA IF EXISTS mojo_migrations_test CASCADE');
  await pool.query('CREATE SCHEMA mojo_migrations_test');

  await t.test('Defaults', async t => {
    const migrations = new Migration('migrations', [], conn);
    t.equal(migrations.name, 'migrations');
    t.equal(await migrations.currentVersion(), 0);
    t.equal(migrations.latest, 0);
  });

  await t.test('Create migrations table', async t => {
    let migrations = new Migration('migrations', [], conn);

    t.same((await getTables(conn)).includes('mojo_migrations_test.mojo_migrations'), false);
    t.equal(await migrations.currentVersion(), 0);

    await migrations.migrate();
    t.same((await getTables(conn)).includes('mojo_migrations_test.mojo_migrations'), true);
    t.equal(await migrations.currentVersion(), 0);

    migrations = new Migration('migrations', [{version: 1, sql: ''}], conn);
    await migrations.migrate();
    t.same((await getTables(conn)).includes('mojo_migrations_test.mojo_migrations'), true);
    t.equal(await migrations.currentVersion(), 1);
  });

  await t.test('Simple migrations', async t => {
    const migrations = new Migration('simple', simpleMigrations, conn);
    t.equal(migrations.latest, 10);
    t.equal(await migrations.currentVersion(), 0);

    await migrations.migrate();
    t.equal(await migrations.currentVersion(), 10);

    t.same((await conn.query('SELECT * FROM migration_test_four')).rows, [{test: 10}]);
  });

  // Clean up once we are done
  await pool.query('DROP SCHEMA mojo_migrations_test CASCADE');

  conn.release();
  await pool.end();
});

interface TablesResult {
  schemaname: string;
  tablename: string;
}

async function getTables(conn: PoolClient): Promise<string[]> {
  const results = await conn.query<TablesResult>(`
    SELECT schemaname, tablename FROM pg_catalog.pg_tables
    WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'`);
  return results.rows.map(row => `${row.schemaname}.${row.tablename}`);
}

const simpleMigrations: MigrationStep[] = [
  {
    version: 7,
    sql: `CREATE TABLE migration_test_four (test INT);`,
  }, {
    version: 10,
    sql: `INSERT INTO migration_test_four VALUES (10);`,
  }
];
