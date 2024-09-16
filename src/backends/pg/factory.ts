import pg from 'pg';


export function createPool(config: string): pg.Pool {
  pg.types.setTypeParser(20, parseInt);
  return new pg.Pool({allowExitOnIdle: true, ...parseConfig(config)})
}

/**
 * Parse PostgreSQL connection URI.
 */
function parseConfig(config: string): pg.PoolConfig {
  const url = new URL(config);
  if (url.protocol.match('/^postgres(ql)?:$/')) throw new TypeError(`Invalid URL: ${config}`);

  const poolConfig: pg.PoolConfig = {};
  if (url.hostname !== '') poolConfig.host = decodeURIComponent(url.hostname);
  if (url.port !== '') poolConfig.port = parseInt(url.port);
  if (url.username !== '') poolConfig.user = url.username;
  if (url.password !== '') poolConfig.password = url.password;
  if (url.pathname.startsWith('/')) poolConfig.database = decodeURIComponent(url.pathname.slice(1));
  const currentSchema = url.searchParams.get('currentSchema');
  if (currentSchema !== null) poolConfig.options = `-c search_path=${currentSchema}`;

  return poolConfig;
}
