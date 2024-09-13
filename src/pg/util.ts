export function throwWithContext(error: any, query: string): never {
  if (error.position !== undefined) {
    const pos = parseInt(error.position);
    let offset = 0;
    let line = 1;
    let lineOffset = 0;
    let fragment = '';

    const sql = query;
    for (let i = 0; i < sql.length; i++) {
      const c = sql[i];
      if (i < pos) {
        lineOffset++;
        if (c === '\n') {
          fragment = '';
          line++;
          offset += lineOffset;
          lineOffset = 0;
        } else {
          fragment += c;
        }
      } else {
        if (c === '\n') {
          break;
        } else {
          fragment += c;
        }
      }
    }

    // SQL context
    const prefix = `Line ${line}: `;
    let pointer = ' '.repeat(pos - offset + prefix.length - 1) + '^';

    error.message = `${error.message}\n${prefix}${fragment}\n${pointer}\n`;
  }

  throw error;
}
