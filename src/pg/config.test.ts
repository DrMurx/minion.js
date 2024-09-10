import {parseConfig} from './config.js';
import t from 'tap';

t.test('parseConfig', t => {
  t.test('Pass-through', t => {
    t.same(parseConfig({database: 'test'}), {database: 'test'});
    t.end();
  });

  t.test('Minimal connection string with database', t => {
    t.same(parseConfig('postgresql:///test1'), {database: 'test1'});
    t.same(parseConfig({database: 'test1'}), {database: 'test1'});

    t.end();
  });

  t.test('Connection string with host and port', t => {
    t.same(parseConfig('postgresql://127.0.0.1:8080/test2'), {host: '127.0.0.1', port: 8080, database: 'test2'});
    t.end();
  });

  t.test('Connection string username but without host', t => {
    t.same(parseConfig('postgres://postgres@/test3'), {user: 'postgres', database: 'test3'});
    t.end();
  });

  t.test('Connection string with unix domain socket', t => {
    t.same(parseConfig('postgresql://x1:y2@%2ftmp%2fpg.sock/test4'), {
      user: 'x1',
      password: 'y2',
      host: '/tmp/pg.sock',
      database: 'test4'
    });
    t.same(parseConfig('postgresql://x1:y2@/test5?host=/tmp/pg.sock'), {
      user: 'x1',
      password: 'y2',
      host: '/tmp/pg.sock',
      database: 'test5'
    });
    t.end();
  });

  t.test('Connection string with lots of zeros', t => {
    t.same(parseConfig('postgresql://0:0@/0'), {user: '0', password: '0', database: '0'});
    t.end();
  });

  t.test('Common variations', t => {
    t.same(parseConfig('postgresql://postgres:postgres@postgres:5432/postgres'), {
      user: 'postgres',
      password: 'postgres',
      host: 'postgres',
      port: 5432,
      database: 'postgres'
    });
    t.same(parseConfig('postgresql://postgres:postgres@/postgres'), {
      user: 'postgres',
      password: 'postgres',
      database: 'postgres'
    });
    t.end();
  });

  t.test('Invalid connection string', t => {
    let result: any;
    try {
      parseConfig('http://127.0.0.1');
    } catch (error) {
      result = error;
    }
    t.match(result.message, /Invalid connection string/);

    t.end();
  });

  t.end();
});
