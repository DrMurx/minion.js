import pg from 'pg';

const URL_RE = /^(([^:/?#]+):)?(\/\/([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?/;

/**
 * Parse PostgreSQL connection URI.
 */
export function parseConfig(config: string | pg.PoolConfig | undefined): pg.PoolConfig | undefined {
  if (typeof config !== 'string') return config;
  const poolConfig: pg.PoolConfig = {};

  const url = urlSplit(config);
  if (url === null || (url.scheme !== 'postgres' && url.scheme !== 'postgresql')) {
    throw new Error(`Invalid connection string: ${config}`);
  }

  const authority = url.authority;
  if (authority !== undefined) {
    const hostParts = authority.split('@');

    const host = hostParts[hostParts.length - 1].split(':');
    if (host[0].length > 0) poolConfig.host = decodeURIComponent(host[0]);
    if (host[1] !== undefined) poolConfig.port = parseInt(host[1]);

    if (hostParts.length > 1) {
      const auth = hostParts[0].split(':');
      poolConfig.user = decodeURIComponent(auth[0]);
      if (auth[1] !== undefined) poolConfig.password = decodeURIComponent(auth[1]);
    }
  }

  const path = url.path;
  if (path !== undefined) poolConfig.database = decodeURIComponent(path.slice(1));

  const params = new URLSearchParams(url.query);
  const host = params.get('host');
  if (host !== null) poolConfig.host = host;

  return poolConfig;
}

interface URLParts {
  authority: string;
  fragment: string;
  path: string;
  query: string;
  scheme: string;
}

function urlSplit(url:string): URLParts | null {
  const match = url.match(URL_RE);
  if (match === null)
      return null;
  return {
      scheme: match[2] ?? '',
      authority: match[4] ?? '',
      path: match[5] ?? '',
      query: match[7] ?? '',
      fragment: match[9] ?? ''
  };
}

