import t from 'tap';
import { Migration, type MigrationStep, migrationTableName } from './migration.js';
import { runTestsWithPgContainer } from './test-suite/container-runner.js';
import { PgBackend } from './backend.js';

await runTestsWithPgContainer(t, PgBackend, async (t, backend) => {
  const conn = await backend.pool.connect();
  conn.on('error', (e) => {
    console.error(`client: ${e.message}`);
    t.fail(`Error from Pg.Client: ${e.message}`);
  });

  await t.test('PostgreSQL migrations', async (t) => {
    await t.test('Defaults', async (t) => {
      const migrations = new Migration('migrations', [], conn);
      t.equal(migrations.name, 'migrations');
      t.equal(await migrations.currentVersion(), 0);
      t.equal(migrations.latest, 0);
    });

    await t.test('Create migration table only when we have something to migrate', async (t) => {
      const testTableSql = `SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = '${migrationTableName}'`;

      const nullMigration: MigrationStep[] = [];
      const migrations = new Migration('null', nullMigration, conn);

      t.same((await conn.query(testTableSql)).rowCount, 0);
      t.equal(await migrations.currentVersion(), 0);

      await migrations.migrate();

      t.same((await conn.query(testTableSql)).rowCount, 0);
      t.equal(await migrations.currentVersion(), 0);

      const emptyMigration: MigrationStep[] = [{ version: 1, sql: '' }];
      const migrations2 = new Migration('emptystep', emptyMigration, conn);
      await migrations2.migrate();

      t.same((await conn.query(testTableSql)).rowCount, 1);
      t.equal(await migrations.currentVersion(), 0);
    });

    await t.test('Simple migrations', async (t) => {
      const simpleMigrations: MigrationStep[] = [
        {
          version: 7,
          sql: `CREATE TABLE migration_test_four (test INT);`,
        },
        {
          version: 10,
          sql: `INSERT INTO migration_test_four VALUES (10);`,
        },
      ];

      const migrations = new Migration('simple', simpleMigrations, conn);
      t.equal(migrations.latest, 10);
      t.equal(await migrations.currentVersion(), 0);

      await migrations.migrate();
      t.equal(await migrations.currentVersion(), 10);

      t.same((await conn.query('SELECT * FROM migration_test_four')).rows, [{ test: 10 }]);
    });
  });

  conn.release();

  // Need to call backend.end - it's usually done by Queue which we don't have here
  await backend.end();
});
