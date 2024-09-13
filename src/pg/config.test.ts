import {parseConfig} from './config.js';
import t from 'tap';

t.test('parseConfig', t => {
  t.test('Minimal connection string with database', t => {
    t.same(parseConfig('postgresql:///test1'), {database: 'test1'});
    t.end();
  });

  t.test('Connection string with host and port', t => {
    t.same(parseConfig('postgresql://127.0.0.1:8080/test2'), {host: '127.0.0.1', port: 8080, database: 'test2'});
    t.end();
  });

  t.test('Connection string with unix domain socket', t => {
    t.same(parseConfig('postgresql://x1:y2@%2ftmp%2fpg.sock/test4'), {
      user: 'x1',
      password: 'y2',
      host: '/tmp/pg.sock',
      database: 'test4'
    });
    t.end();
  });

  t.test('Connection string with lots of zeros', t => {
    t.same(parseConfig('postgresql://0:0@0/0'), {user: '0', password: '0', host: '0', database: '0'});
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
    t.end();
  });

  t.test('Invalid connection string', t => {
    let result: any;

    try {
      result = undefined;
      parseConfig('postgres://postgres@/test3');
    } catch (error) {
      result = error;
    }
    t.match(result.message, /Invalid URL/);

    try {
      result = undefined;
      parseConfig('postgresql://postgres:postgres@/postgres');
    } catch (error) {
      result = error;
    }
    t.match(result.message, /Invalid URL/);

    try {
      result = undefined;
      parseConfig('http://127.0.0.1');
    } catch (error) {
      result = error;
    }
    t.match(result.message, /Invalid URL/);

    t.end();
  });

  t.end();
});
