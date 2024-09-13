import {Pg} from './pg.js';
import t from 'tap';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};
const pgConfig = process.env.TEST_ONLINE!;

t.test('Results', skip, async t => {
  // Isolate tests
  const pg = new Pg(`${pgConfig}?currentSchema=mojo_results_test`);
  const conn = await pg.getConnection();
  await conn.query('DROP SCHEMA IF EXISTS mojo_results_test CASCADE');
  await conn.query('CREATE SCHEMA mojo_results_test');

  await conn.query(`
    CREATE TABLE IF NOT EXISTS results_test (
      id   BIGSERIAL PRIMARY KEY,
      name TEXT
    )
  `);
  await conn.query('INSERT INTO results_test (name) VALUES ($1)', 'foo');
  await conn.query('INSERT INTO results_test (name) VALUES ($1)', 'bar');

  await t.test('Tables', async t => {
    t.same((await conn.getTables()).includes('mojo_results_test.results_test'), true);
    t.same((await conn.getTables()).includes('information_schema.tables'), false);
    t.same((await conn.getTables()).includes('pg_catalog.pg_tables'), false);
  });

  await t.test('Result methods', async t => {
    t.same(await conn.query('SELECT * FROM results_test'), [
      {id: 1, name: 'foo'},
      {id: 2, name: 'bar'}
    ]);
    t.same((await conn.query('SELECT * FROM results_test')).first, {id: 1, name: 'foo'});
    t.same((await conn.query('SELECT * FROM results_test')).last, {id: 2, name: 'bar'});
    t.same(await conn.query('SELECT * FROM results_test WHERE name = $1', 'baz'), []);
    t.same((await conn.query('SELECT * FROM results_test WHERE name = $1', 'baz')).first, undefined);
    t.same((await conn.query('SELECT * FROM results_test WHERE name = $1', 'baz')).last, undefined);
    t.same(await conn.query('SELECT * FROM results_test WHERE name = $1', 'bar'), [{id: 2, name: 'bar'}]);
    t.same((await conn.query('SELECT * FROM results_test')).count, 2);
    t.same((await conn.query('SHOW SERVER_VERSION')).count, null);
  });

  await t.test('JSON', async t => {
    t.same((await conn.query('SELECT $1::JSON AS foo', {bar: 'baz'})).first, {foo: {bar: 'baz'}});
  });

  await t.test('Transactions', async t => {
    await conn.query('BEGIN');
    try {
      await conn.query("INSERT INTO results_test (name) VALUES ('tx1')");
      await conn.query("INSERT INTO results_test (name) VALUES ('tx1')");
      await conn.query('COMMIT');
    } finally {
      await conn.query('ROLLBACK');
    }
    t.same(await conn.query('SELECT * FROM results_test WHERE name = $1', 'tx1'), [
      {id: 3, name: 'tx1'},
      {id: 4, name: 'tx1'}
    ]);

    await conn.query('BEGIN');
    try {
      await conn.query("INSERT INTO results_test (name) VALUES ('tx1')");
      await conn.query("INSERT INTO results_test (name) VALUES ('tx1')");
    } finally {
      await conn.query('ROLLBACK');
    }
    t.same(await conn.query('SELECT * FROM results_test WHERE name = $1', 'tx1'), [
      {id: 3, name: 'tx1'},
      {id: 4, name: 'tx1'}
    ]);

    let result: any;
    await conn.query('BEGIN');
    try {
      await conn.query("INSERT INTO results_test (name) VALUES ('tx1')");
      await conn.query("INSERT INTO results_test (name) VALUES ('tx1')");
      await conn.query('does_not_exist');
      await conn.query('COMMIT');
    } catch (error) {
      result = error;
    } finally {
      await conn.query('ROLLBACK');
    }
    t.match(result.message, /does_not_exist/);
    t.same(await conn.query('SELECT * FROM results_test WHERE name = $1', 'tx1'), [
      {id: 3, name: 'tx1'},
      {id: 4, name: 'tx1'}
    ]);
  });

  // Clean up once we are done
  await conn.query('DROP SCHEMA mojo_results_test CASCADE');

  await conn.release();
  await pg.end();
});
