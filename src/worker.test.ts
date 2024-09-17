import t from 'tap';
import { PgBackend } from './backends/pg/backend.js';
import { createPool } from './backends/pg/factory.js';
import { Minion } from './index.js';

const skip = process.env.TEST_ONLINE === undefined ? {skip: 'set TEST_ONLINE to enable this test'} : {};
const pgConfig = process.env.TEST_ONLINE!;

t.test('Worker', skip, async t => {
  const pool = createPool(`${pgConfig}?currentSchema=minion_worker_test`)

  // Isolate tests
  await pool.query('DROP SCHEMA IF EXISTS minion_worker_test CASCADE');
  await pool.query('CREATE SCHEMA minion_worker_test');

  const backend = new PgBackend(pool);
  const minion = new Minion(backend);
  await minion.updateSchema();

  minion.addTask('test', async job => {
    await job.addNotes({test: 'pass'});
  });

  const worker = await minion.createWorker().start();
  t.equal(worker.isRunning, true);

  await t.test('Wait for jobs', async t => {
    const id = await minion.addJob('test');
    const result = (await minion.getJobResult(id, {interval: 500}))!;
    t.equal(result.state, 'succeeded');
    t.same(result.notes, {test: 'pass'});
  });

  t.equal(worker.isRunning, true);
  await worker.stop();
  t.equal(worker.isRunning, false);

  // Clean up once we are done
  await pool.query('DROP SCHEMA minion_worker_test CASCADE');

  await pool.end();
});
