import {Pg} from './pg.js';
import t from 'tap';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};
const pgConfig = process.env.TEST_ONLINE!;

interface TestRecord {
  id: number;
  name: string;
}

t.test('Results', skip, async t => {
  // Isolate tests
  await using pg = new Pg(`${pgConfig}?currentSchema=mojo_ts_results_test`);
  await using db = await pg.getConnection();
  await db.query('DROP SCHEMA IF EXISTS mojo_ts_results_test CASCADE');
  await db.query('CREATE SCHEMA mojo_ts_results_test');

  await db.query(`
    CREATE TABLE IF NOT EXISTS results_test (
      id   BIGSERIAL PRIMARY KEY,
      name TEXT
    )
  `);
  await db.query('INSERT INTO results_test (name) VALUES ($1)', 'foo');
  await db.query('INSERT INTO results_test (name) VALUES ($1)', 'bar');

  await t.test('Result methods', async t => {
    t.same(await db.query<TestRecord>('SELECT * FROM results_test'), [
      {id: 1, name: 'foo'},
      {id: 2, name: 'bar'}
    ]);
    t.same((await db.query<TestRecord>('SELECT * FROM results_test')).first, {id: 1, name: 'foo'});
    t.same((await db.query<TestRecord>('SELECT * FROM results_test')).count, 2);
  });

  // Clean up once we are done
  await db.query('DROP SCHEMA mojo_ts_results_test CASCADE');
});
