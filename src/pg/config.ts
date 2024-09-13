import { URL } from 'node:url';
import pg from 'pg';

/**
 * Parse PostgreSQL connection URI.
 */
export function parseConfig(config: string): pg.PoolConfig {
  const url = new URL(config);
  if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:') {
    throw new TypeError(`Invalid URL: ${config}`);
  }

  const poolConfig: pg.PoolConfig = {};
  if (url.hostname !== '') {
    poolConfig.host = decodeURIComponent(url.hostname);
  }
  if (url.port !== '') {
    poolConfig.port = parseInt(url.port);
  }
  if (url.username !== '') {
    poolConfig.user = url.username;
  }
  if (url.password !== '') {
    poolConfig.password = url.password;
  }
  if (url.pathname.startsWith('/')) {
    poolConfig.database = decodeURIComponent(url.pathname.slice(1));
  }
  return poolConfig;
}
