const URL_RE = /^(([^:/?#]+):)?(\/\/([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?/;

export interface URLParts {
  authority: string;
  fragment: string;
  path: string;
  query: string;
  scheme: string;
}

export default function urlSplit(url:string): URLParts | null {
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

