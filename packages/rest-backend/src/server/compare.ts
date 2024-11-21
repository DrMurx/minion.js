import { timingSafeEqual } from 'crypto';

/**
 * Wrapper around `crypto.timingSafeEqual` fixing its limitations (equal length of both parameters)
 */
export function timingSafeCompare(a: Buffer, b: Buffer): boolean {
  return a.length === b.length ? timingSafeEqual(a, b) : !timingSafeEqual(a, a);
}
