import {Pg} from './pg.js';
import t from 'tap';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};

t.test('Database', skip, async t => {
  await t.test('Options', async t => {
    const pg = new Pg(process.env.TEST_ONLINE, {
      allowExitOnIdle: true,
      connectionTimeoutMillis: 10000,
      idleTimeoutMillis: 20000,
      max: 1
    });
    t.equal(pg.pool.options.allowExitOnIdle, true);
    t.equal(pg.pool.options.connectionTimeoutMillis, 10000);
    t.equal(pg.pool.options.idleTimeoutMillis, 20000);
    t.equal(pg.pool.options.max, 1);
    await pg.end();
  });

  await t.test('Close connection', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);

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
    const pg = new Pg(process.env.TEST_ONLINE);
    const conn = await pg.getConnection();
    t.ok(typeof (await conn.getBackendPid()) === 'number');
    await conn.release();
    await pg.end();
  });

  await t.test('Select (ad-hoc)', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);

    const results = await pg.query('SELECT 1 AS one, 2 AS two, 3 AS three');
    t.equal(results.count, 1);
    t.same(results, [{one: 1, two: 2, three: 3}]);

    const results2 = await pg.query('SELECT 1, 2, 3');
    t.equal(results2.count, 1);
    t.same(results2, [{'?column?': 3}]);

    await pg.end();
  });

  await t.test('Select (with database object)', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);
    const conn = await pg.getConnection();
    const results = await conn.query('SELECT 1 AS one, 2 AS two, 3 AS three');
    t.equal(results.count, 1);
    t.same(results, [{one: 1, two: 2, three: 3}]);
    const results2 = await conn.query('SELECT 1 AS one');
    t.same(results2, [{one: 1}]);
    await conn.release();
    await pg.end();
  });

  await t.test('Custom search path', async t => {
    const pg = new Pg(process.env.TEST_ONLINE, {searchPath: ['$user', 'foo', 'bar']});
    const results = await pg.query('SHOW search_path');
    t.same(results, [{search_path: '"$user", foo, bar'}]);
    await pg.end();
  });

  await t.test('Connection reuse', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);
    const conn = await pg.getConnection();
    await conn.release();
    const conn2 = await pg.getConnection();
    t.ok(conn.client === conn2.client);
    await conn2.release();
    await pg.end();
  });

  await t.test('Concurrent selects (ad-hoc)', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);

    const all = await Promise.all([pg.query('SELECT 1 AS one'), pg.query('SELECT 2 AS two'), pg.query('SELECT 3 AS three')]);
    t.same(
      all.map(results => results),
      [[{one: 1}], [{two: 2}], [{three: 3}]]
    );

    await pg.end();
  });

  await t.test('Concurrent selects (with database objects)', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);
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
    const pg = new Pg(process.env.TEST_ONLINE);
    const results = await pg.query('SELECT $1 AS one', 'One');
    t.same(results, [{one: 'One'}]);
    const results2 = await pg.query('SELECT $1 AS one, $2 AS two', 'One', 2);
    t.same(results2, [{one: 'One', two: 2}]);
    await pg.end();
  });

  await t.test('JSON', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);
    const results = await pg.query('SELECT $1::JSON AS foo', {test: ['works']});
    t.same(results, [{foo: {test: ['works']}}]);
    await pg.end();
  });

  await t.test('With query config objects', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);
    const results = await pg.query({text: 'SELECT $1 AS one', values: ['One']});
    t.same(results, [{one: 'One'}]);
    const results2 = await pg.query({text: 'SELECT $1 AS one', values: ['One'], rowMode: 'array'});
    t.same(results2, [['One']]);
    await pg.end();
  });

  await t.test('With query config objects (with database object)', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);
    const conn = await pg.getConnection();
    const results = await conn.query({text: 'SELECT $1 AS one', values: ['One']});
    t.same(results, [{one: 'One'}]);
    const results2 = await conn.query({text: 'SELECT $1 AS one', values: ['One'], rowMode: 'array'});
    t.same(results2, [['One']]);
    const results3 = await conn.query('SELECT $1 AS one', 'One');
    t.same(results3, [{one: 'One'}]);
    await conn.release();
    await pg.end();
  });

  await t.test('Notifications (two database objects)', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);

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
    const tx = await conn.startTransaction();
    let result;
    try {
      process.nextTick(async () => {
        await conn.notify('dbtest', 'from a transaction');
        await tx.commit();
      });
      const message3: any = await new Promise(resolve => conn.once('notification', message => resolve(message)));
      result = [message3.channel, message3.payload];
    } catch (error) {
      result = error;
      await tx.rollback();
    }
    t.same(result, ['dbtest', 'from a transaction']);

    await conn.release();
    await conn2.release();

    await pg.end();
  });

  await t.test('Notifications (iterator)', async t => {
    const pg = new Pg(process.env.TEST_ONLINE);

    const conn = await pg.getConnection();
    await conn.listen('dbtest2');
    await conn.listen('dbtest3');

    const messages = [];
    process.nextTick(() => conn.notify('dbtest2', 'works'));
    for await (const message of conn) {
      messages.push(message);
      break;
    }
    await conn.unlisten('dbtest2');
    process.nextTick(async () => {
      await conn.notify('dbtest3', 'maybe');
      await conn.notify('dbtest2', 'failed');
      await conn.notify('dbtest3', 'too');
    });
    for await (const message of conn) {
      messages.push(message);
      if (messages.length > 2) break;
    }
    t.same(
      messages.map(message => [message.channel, message.payload]),
      [
        ['dbtest2', 'works'],
        ['dbtest3', 'maybe'],
        ['dbtest3', 'too']
      ]
    );

    await t.test('Exception with context (ad-hoc)', async t => {
      const pg = new Pg(process.env.TEST_ONLINE);

      let result: any;
      try {
        await pg.query(`
          SELECT 1 AS one,
                 2 A two,
                 3 AS three
        `);
      } catch (error) {
        result = error;
      }
      t.match(result.message, /syntax error at or near "two".+Line 3: {18}2 A two,/s);

      result = undefined;
      try {
        await pg.query('SELECT 1 A one');
      } catch (error) {
        result = error;
      }
      t.match(result.message, /syntax error at or near "one".+Line 1: SELECT 1 A one/s);

      await pg.end();
    });

    await t.test('Exception (with connection object)', async t => {
      const pg = new Pg(process.env.TEST_ONLINE);
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
      t.match(result.message, /syntax error at or near "two".+Line 3: {18}2 A two,/s);

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

    await conn.release();

    await pg.end();
  });
});
