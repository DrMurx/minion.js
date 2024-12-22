import { PostgreSqlContainer } from '@testcontainers/postgresql';
import pg from 'pg';
import { Test } from 'tap';
import { Wait } from 'testcontainers';
import { PgBackend } from '../backend.js';

export async function runTestsWithPgContainer<Backend extends PgBackend = PgBackend>(
  t: Test,
  BackendClazz: new (config: string | URL | pg.Pool) => Backend,
  testRunner: (t: Test, backend: Backend) => Promise<void>,
): Promise<void> {
  const tapTimeout = t.options.timeout ?? 30000;

  // Fire up PostgreSql container and wait until default time (60 seconds) it's available.
  // Defer the test timeout also to 60 seconds.
  t.setTimeout(60 * 1000);
  const container = await new PostgreSqlContainer()
    .withWaitStrategy(
      Wait.forAll([
        // Postgres starts twice inside the container
        Wait.forLogMessage(/database system is ready to accept connections/, 2),
        // Port must be available
        Wait.forListeningPorts(),
      ]),
    )
    .start();

  // Restart the test timeout with what it was before
  t.setTimeout(tapTimeout);

  const config = container.getConnectionUri();
  const backend = new BackendClazz(config);
  backend.pool.on('error', (e) => {
    console.log(e.message);
    t.fail(`Error from Pg.Pool: ${e.message}`);
  });

  await testRunner(t, backend);

  await container.stop();
}
