import {Pg} from './pg.js';
import {dirname, join} from 'path';
import t from 'tap';
import { fileURLToPath } from 'url';
import { Migrations } from './migrations.js';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};

t.test('Migrations', skip, async t => {
  // Isolate tests
  const pg = new Pg(process.env.TEST_ONLINE, {searchPath: ['mojo_migrations_test']});
  await pg.query('DROP SCHEMA IF EXISTS mojo_migrations_test CASCADE');
  await pg.query('CREATE SCHEMA mojo_migrations_test');
  const migrations = new Migrations(pg);

  await t.test('Defaults', async t => {
    t.equal(migrations.name, 'migrations');
    t.equal(await migrations.currentVersion(), 0);
    t.equal(migrations.latest, 0);
  });

  await t.test('Create migrations table', async t => {
    t.same((await pg.getTables()).includes('mojo_migrations_test.mojo_migrations'), false);
    t.equal(await migrations.currentVersion(), 0);

    await migrations.migrateTo();
    t.same((await pg.getTables()).includes('mojo_migrations_test.mojo_migrations'), false);
    t.equal(await migrations.currentVersion(), 0);

    migrations.loadFromString('-- 1 up\n\n');
    await migrations.migrateTo();
    t.same((await pg.getTables()).includes('mojo_migrations_test.mojo_migrations'), true);
    t.equal(await migrations.currentVersion(), 1);
  });

  await t.test('Simple migrations', async t => {
    migrations.name = 'simple';
    migrations.loadFromString(simpleMigrations);
    t.equal(migrations.latest, 10);
    t.equal(await migrations.currentVersion(), 0);

    const sql = migrations.sqlFor(0, 10);
    t.match(sql, /CREATE TABLE migration_test_four/s);
    await migrations.migrateTo();
    t.equal(await migrations.currentVersion(), 10);

    t.same(await pg.query('SELECT * FROM migration_test_four'), [{test: 10}]);
  });

  await t.test('Different stntax variations', async t => {
    migrations.name = 'syntax_variations';
    migrations.loadFromString(syntaxVariations);
    t.equal(migrations.latest, 10);
    t.equal(await migrations.currentVersion(), 0);
    await migrations.migrateTo();
    t.same((await pg.getTables()).includes('mojo_migrations_test.migration_test_one'), true);
    t.same((await pg.getTables()).includes('mojo_migrations_test.migration_test_two'), true);
    t.same(await pg.query('SELECT * FROM migration_test_one'), [{foo: 'works ♥'}]);
    t.equal(await migrations.currentVersion(), 10);

    await migrations.migrateTo(1);
    t.equal(await migrations.currentVersion(), 1);
    t.same(await pg.query('SELECT * FROM migration_test_one'), []);

    await migrations.migrateTo(3);
    t.equal(await migrations.currentVersion(), 3);
    t.same(await pg.query('SELECT * FROM migration_test_one'), [{foo: 'works ♥'}]);
    t.same(await pg.query('SELECT * FROM migration_test_two'), []);

    await migrations.migrateTo(10);
    t.equal(await migrations.currentVersion(), 10);
    t.same(await pg.query('SELECT * FROM migration_test_two'), [{bar: 'works too'}]);

    await migrations.migrateTo(0);
    t.equal(await migrations.currentVersion(), 0);
  });

  await t.test('Bad and concurrent migrations', async t => {
    const pg2 = new Pg(process.env.TEST_ONLINE, {searchPath: ['mojo_migrations_test']});
    const migrations2 = new Migrations(pg2);
    const file = join(dirname(fileURLToPath(import.meta.url)), 'support', 'migrations', 'test.sql');
    await migrations2.loadFromFile(file, {name: 'migrations_test2'});
    t.equal(migrations2.latest, 4);
    t.equal(await migrations2.currentVersion(), 0);

    let result: any;
    try {
      await migrations2.migrateTo();
    } catch (error) {
      result = error;
    }
    t.match(result.message, /does_not_exist/);
    t.equal(await migrations2.currentVersion(), 0);

    await migrations2.migrateTo(3);
    t.equal(await migrations2.currentVersion(), 3);

    await migrations2.migrateTo(2);
    t.equal(await migrations2.currentVersion(), 2);

    t.equal(await migrations.currentVersion(), 0);
    await migrations.migrateTo();
    t.equal(await migrations.currentVersion(), 10);
    t.same((await pg.query('SELECT * FROM migration_test_three')).first, {baz: 'just'});
    t.same(await pg.query('SELECT * FROM migration_test_three'), [{baz: 'just'}, {baz: 'works ♥'}]);
    t.same((await pg.query('SELECT * FROM migration_test_three')).last, {baz: 'works ♥'});

    await migrations.migrateTo(0);
    t.equal(await migrations.currentVersion(), 0);
    await migrations2.migrateTo(0);
    t.equal(await migrations2.currentVersion(), 0);

    await pg2.end();
  });

  await t.test('Unknown version', async t => {
    let result: any;
    try {
      await migrations.migrateTo(23);
    } catch (error) {
      result = error;
    }
    t.match(result.message, /Version 23 has no migration/);
  });

  await t.test('Version mismatch', async t => {
    migrations.loadFromString(newerVersion, {name: 'migrations_test3'});
    await migrations.migrateTo();
    t.equal(await migrations.currentVersion(), 2);

    migrations.loadFromString(olderVersion);
    let result: any;
    try {
      await migrations.migrateTo();
    } catch (error) {
      result = error;
    }
    t.match(result.message, /Active version 2 is greater than the latest version 1/);

    let result2: any;
    try {
      await migrations.migrateTo(0);
    } catch (error) {
      result2 = error;
    }
    t.match(result2.message, /Active version 2 is greater than the latest version 1/);
  });

  await t.test('Migration directory', async t => {
    const pg2 = new Pg(process.env.TEST_ONLINE, {searchPath: ['mojo_migrations_test']});
    const migrations2 = new Migrations(pg2);
    const dir = join(dirname(fileURLToPath(import.meta.url)), 'support', 'migrations', 'tree');
    await migrations2.loadFromDirectory(dir, {name: 'directory tree'});
    t.same((await pg2.getTables()).includes('mojo_migrations_test.migration_test_three'), false);
    await migrations2.migrateTo(2);
    t.same((await pg2.getTables()).includes('mojo_migrations_test.migration_test_three'), true);
    t.equal(await migrations2.currentVersion(), 2);
    t.same(await pg2.query('SELECT * FROM migration_test_three'), [{baz: 'just'}, {baz: 'works ♥'}]);

    let result: any;
    try {
      await migrations.migrateTo(36);
    } catch (error) {
      result = error;
    }
    t.match(result.message, /Version 36 has no migration/);

    let result2: any;
    try {
      await migrations.migrateTo(54);
    } catch (error) {
      result2 = error;
    }
    t.match(result2.message, /Version 54 has no migration/);

    let result3: any;
    try {
      await migrations.migrateTo(55);
    } catch (error) {
      result3 = error;
    }
    t.match(result3.message, /Version 55 has no migration/);

    await migrations2.migrateTo(99);
    t.equal(await migrations2.currentVersion(), 99);
    t.same((await pg2.getTables()).includes('mojo_migrations_test.migration_test_luft_balloons'), true);

    const dir2 = join(dirname(fileURLToPath(import.meta.url)), 'support', 'migrations', 'tree2');
    await migrations2.loadFromDirectory(dir2);
    t.equal(migrations2.latest, 8);

    await pg2.end();
  });

  // Clean up once we are done
  await pg.query('DROP SCHEMA mojo_migrations_test CASCADE');

  await pg.end();
});

const simpleMigrations = `
-- 7 up
CREATE TABLE migration_test_four (test INT);

-- 10 up
INSERT INTO migration_test_four VALUES (10);
`;

const syntaxVariations = `
-- 1 up
CREATE TABLE IF NOT EXISTS migration_test_one (foo VARCHAR(255));

-- 1down

  DROP TABLE IF EXISTS migration_test_one;

  -- 2 up

INSERT INTO migration_test_one VALUES ('works ♥');
-- 2 down
DELETE FROM migration_test_one WHERE foo = 'works ♥';
--
--  3 Up, create
--        another
--        table?
CREATE TABLE IF NOT EXISTS migration_test_two (bar VARCHAR(255));
--3  DOWN
DROP TABLE IF EXISTS migration_test_two;

-- 10 up (not down)
INSERT INTO migration_test_two VALUES ('works too');
-- 10 down (not up)
DELETE FROM migration_test_two WHERE bar = 'works too';
`;

const newerVersion = `
-- 2 up
CREATE TABLE migration_test_five (test INT);
-- 2 down
DROP TABLE migration_test_five;
`;

const olderVersion = `
-- 1 up
CREATE TABLE migration_test_five (test INT);
`;
