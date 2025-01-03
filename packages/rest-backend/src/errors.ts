import { ConnectionError } from '@queuebone/core';

export class AuthenticationError extends ConnectionError {
  constructor(
    message: string,
    private _id: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.code = 'AUTHENTICATION_ERROR';
    this.name = 'AuthenticationError';
  }

  get id(): string {
    return this._id;
  }
}
