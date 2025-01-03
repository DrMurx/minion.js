export interface QueueboneErrorOptions extends ErrorOptions {
  code?: string;
}

export class QueueboneError extends Error {
  protected _code: string;

  constructor(message: string, options?: QueueboneErrorOptions) {
    super(message, options);
    this._code = options?.code ?? 'UNKNOWN_ERROR';
    this.name = 'QueueboneError';
  }

  get code(): string {
    return this._code;
  }
}

export class ConfigurationError extends QueueboneError {
  constructor(message: string = 'Invalid configuration', options?: QueueboneErrorOptions) {
    super(message, { code: 'INVALID_CONFIGURATION', ...options });
    this.name = 'ConfigurationError';
  }
}

export class InvalidStateError extends QueueboneError {
  constructor(message: string = 'Invalid state', options?: QueueboneErrorOptions) {
    super(message, { code: 'INVALID_STATE', ...options });
    this.name = 'InvalidStateError';
  }
}

export class UnsupportedOperationError extends QueueboneError {
  constructor(message: string = 'Unsupported operation', options?: QueueboneErrorOptions) {
    super(message, { code: 'UNSUPPORTED_OPERATION', ...options });
    this.name = 'UnsupportedOperationError';
  }
}

export class ConnectionError extends QueueboneError {
  constructor(message: string, options?: QueueboneErrorOptions) {
    super(message, { code: 'CONNECTION_ERROR', ...options });
    this.name = 'ConnectionError';
  }
}
