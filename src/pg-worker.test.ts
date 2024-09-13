import {Minion} from './index.js';
import { PgBackend } from './pg-backend.js';
import {Pg} from './pg/pg.js';
import t from 'tap';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};
const pgConfig = process.env.TEST_ONLINE!;

t.test('Worker', skip, async t => {
  // Isolate tests
  const pg = new Pg(pgConfig, {searchPath: ['minion_worker_test']});
  await pg.query('DROP SCHEMA IF EXISTS minion_worker_test CASCADE');
  await pg.query('CREATE SCHEMA minion_worker_test');

  const minion = new Minion(pg, {backendClass: PgBackend});
  await minion.updateSchema();

  minion.addTask('test', async job => {
    await job.addNotes({test: 'pass'});
  });

  const worker = await minion.createWorker().start();
  t.equal(worker.isRunning, true);

  await t.test('Wait for jobs', async t => {
    const id = await minion.addJob('test');
    const result = await minion.getJobResult(id, {interval: 500});
    t.equal(result!.state, 'finished');
    t.same(result!.notes, {test: 'pass'});
  });

  t.equal(worker.isRunning, true);
  await worker.stop();
  t.equal(worker.isRunning, false);

  // Clean up once we are done
  await pg.query('DROP SCHEMA minion_worker_test CASCADE');

  await pg.end();
});
