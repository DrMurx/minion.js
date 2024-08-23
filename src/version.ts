import Path from '@mojojs/path';

export const version = JSON.parse(
  Path.currentFile().dirname().sibling('package.json').readFileSync().toString()
).version;
