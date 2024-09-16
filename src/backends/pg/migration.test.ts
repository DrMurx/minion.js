import { PoolClient } from 'pg';
import t from 'tap';
import { createPool } from './factory.js';
import { Migration, MigrationStep, migrationTableName } from './migration.js';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};
const pgConfig = process.env.TEST_ONLINE!;

t.test('Migrations', skip, async t => {
  // Isolate tests
  const isolationSchema = 'queue_migrations_test';
  const pool = createPool(`${pgConfig}?currentSchema=${isolationSchema}`);

  await pool.query(`DROP SCHEMA IF EXISTS ${isolationSchema} CASCADE`);
  await pool.query(`CREATE SCHEMA ${isolationSchema}`);

  const conn = await pool.connect();

  await t.test('Defaults', async t => {
    const migrations = new Migration('migrations', [], conn);
    t.equal(migrations.name, 'migrations');
    t.equal(await migrations.currentVersion(), 0);
    t.equal(migrations.latest, 0);
  });

  await t.test('Create migration table only when we have something to migrate', async t => {
    const testTableSql = `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '${isolationSchema}' AND tablename = '${migrationTableName}'`;

    const nullMigration: MigrationStep[] = [];
    const migrations = new Migration('null', nullMigration, conn);

    t.same((await conn.query(testTableSql)).rowCount, 0);
    t.equal(await migrations.currentVersion(), 0);

    await migrations.migrate();

    t.same((await conn.query(testTableSql)).rowCount, 0);
    t.equal(await migrations.currentVersion(), 0);

    const emptyMigration: MigrationStep[] = [{version: 1, sql:''}];
    const migrations2 = new Migration('emptystep', emptyMigration, conn);
    await migrations2.migrate();

    t.same((await conn.query(testTableSql)).rowCount, 1);
    t.equal(await migrations.currentVersion(), 0);
  });

  await t.test('Simple migrations', async t => {
    const simpleMigrations: MigrationStep[] = [
      {
        version: 7,
        sql: `CREATE TABLE migration_test_four (test INT);`,
      }, {
        version: 10,
        sql: `INSERT INTO migration_test_four VALUES (10);`,
      }
    ];

    const migrations = new Migration('simple', simpleMigrations, conn);
    t.equal(migrations.latest, 10);
    t.equal(await migrations.currentVersion(), 0);

    await migrations.migrate();
    t.equal(await migrations.currentVersion(), 10);

    t.same((await conn.query('SELECT * FROM migration_test_four')).rows, [{test: 10}]);
  });

  // Clean up once we are done
  await pool.query(`DROP SCHEMA ${isolationSchema} CASCADE`);

  conn.release();
  await pool.end();
});
