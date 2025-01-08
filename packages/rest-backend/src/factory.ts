import axios, { type Axios } from 'axios';

export function createAxios(config: string | URL): Axios {
  const url = parseConfig(config);
  return axios.create({
    baseURL: `${url.origin}${url.pathname}`,
  });
}

/**
 * Parse REST Backend connection URL.
 */
export function parseConfig(config: string | URL): URL {
  const url = new URL(config);
  if (url.protocol.match(/^https?:$/) === null) {
    throw new TypeError(`Invalid URL: ${config}`);
  }

  return url;
}
