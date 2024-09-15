import {Pg} from './pg.js';
import t from 'tap';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};
const pgConfig = process.env.TEST_ONLINE!;

t.test('Connection', skip, async t => {
  await t.test('Close connection', async t => {
    const pg = new Pg(pgConfig);

    const conn = await pg.getConnection();
    let count = 0;
    conn.on('end', () => count++);
    await conn.release();
    t.equal(count, 0);

    const conn2 = await pg.getConnection();
    conn2.on('end', () => count++);
    await conn2.end();
    t.equal(count, 1);
    await conn2.release();

    await pg.end();
  });

  await t.test('Backend process id', async t => {
    const pg = new Pg(pgConfig);
    const conn = await pg.getConnection();
    t.ok(typeof (await conn.getBackendPid()) === 'number');
    await conn.release();
    await pg.end();
  });

  await t.test('Select (with connection object)', async t => {
    const pg = new Pg(pgConfig);
    const conn = await pg.getConnection();
    const results = await conn.query('SELECT 1 AS one, 2 AS two, 3 AS three');
    t.equal(results.count, 1);
    t.same(results, [{one: 1, two: 2, three: 3}]);
    const results2 = await conn.query('SELECT 1 AS one');
    t.same(results2, [{one: 1}]);
    await conn.release();
    await pg.end();
  });

  await t.test('Exception (with connection object)', async t => {
    const pg = new Pg(pgConfig);
    const conn = await pg.getConnection();

    let result: any;
    try {
      await conn.query(`
        SELECT 1 AS one,
               2 A two,
               3 AS three
      `);
    } catch (error) {
      result = error;
    }
    t.match(result.message, /syntax error at or near "two".+Line 3: +2 A two,/s);

    result = undefined;
    try {
      await conn.query('SELECT 1 A one');
    } catch (error) {
      result = error;
    }
    t.match(result.message, /syntax error at or near "one".+Line 1: SELECT 1 A one/s);

    await conn.release();
    await pg.end();
  });

  await t.test('Custom search path', async t => {
    const pg = new Pg(`${pgConfig}?currentSchema="$user",foo,bar`);
    const conn = await pg.getConnection();
    const results = await conn.query('SHOW search_path');
    t.same(results, [{search_path: '"$user",foo,bar'}]);
    await conn.release();
    await pg.end();
  });

  await t.test('Connection reuse', async t => {
    const pg = new Pg(pgConfig);
    const conn = await pg.getConnection();
    await conn.release();
    const conn2 = await pg.getConnection();
    t.ok(conn.client === conn2.client);
    await conn2.release();
    await pg.end();
  });

  await t.test('Concurrent selects (with connection objects)', async t => {
    const pg = new Pg(pgConfig);
    const conn1 = await pg.getConnection();
    const conn2 = await pg.getConnection();
    const conn3 = await pg.getConnection();

    const all = await Promise.all([
      conn1.query('SELECT 1 AS one'),
      conn2.query('SELECT 2 AS two'),
      conn3.query('SELECT 3 AS three')
    ]);
    t.same(
      all.map(results => results),
      [[{one: 1}], [{two: 2}], [{three: 3}]]
    );

    await conn1.release();
    await conn2.release();
    await conn3.release();
    await pg.end();
  });

  await t.test('Placeholders', async t => {
    const pg = new Pg(pgConfig);
    const conn = await pg.getConnection();
    const results = await conn.query('SELECT $1 AS one', 'One');
    t.same(results, [{one: 'One'}]);
    const results2 = await conn.query('SELECT $1 AS one, $2 AS two', 'One', 2);
    t.same(results2, [{one: 'One', two: 2}]);
    await conn.release();
    await pg.end();
  });

  await t.test('JSON', async t => {
    const pg = new Pg(pgConfig);
    const conn = await pg.getConnection();
    const results = await conn.query('SELECT $1::JSON AS foo', {test: ['works']});
    t.same(results, [{foo: {test: ['works']}}]);
    await conn.release();
    await pg.end();
  });

  await t.test('Notifications (two connection objects)', async t => {
    const pg = new Pg(pgConfig);

    const conn = await pg.getConnection();
    const conn2 = await pg.getConnection();
    await conn.listen('dbtest');
    await conn2.listen('dbtest');

    process.nextTick(() => conn.notify('dbtest', 'it works!'));
    const messages: any[] = await Promise.all([
      new Promise(resolve => conn.once('notification', message => resolve(message))),
      new Promise(resolve => conn2.once('notification', message => resolve(message)))
    ]);
    t.same(
      messages.map(message => [message.channel, message.payload]),
      [
        ['dbtest', 'it works!'],
        ['dbtest', 'it works!']
      ]
    );

    process.nextTick(() => conn.notify('dbtest', 'it still works!'));
    const messages2: any[] = await Promise.all([
      new Promise(resolve => conn.once('notification', message => resolve(message))),
      new Promise(resolve => conn2.once('notification', message => resolve(message)))
    ]);
    t.same(
      messages2.map(message => [message.channel, message.payload]),
      [
        ['dbtest', 'it still works!'],
        ['dbtest', 'it still works!']
      ]
    );

    process.nextTick(() => conn2.notify('dbtest'));
    const message2: any = await new Promise(resolve => conn2.once('notification', message => resolve(message)));
    t.same([message2.channel, message2.payload], ['dbtest', '']);

    await conn2.unlisten('dbtest');
    await conn.query('BEGIN');
    let result;
    try {
      process.nextTick(async () => {
        await conn.notify('dbtest', 'from a transaction');
        await conn.query('COMMIT');
      });
      const message3: any = await new Promise(resolve => conn.once('notification', message => resolve(message)));
      result = [message3.channel, message3.payload];
    } catch (error) {
      result = error;
      await conn.query('ROLLBACK');
    }
    t.same(result, ['dbtest', 'from a transaction']);

    await conn.release();
    await conn2.release();

    await pg.end();
  });

});
