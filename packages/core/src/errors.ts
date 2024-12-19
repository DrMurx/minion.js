export class QueueboneError extends Error {
  constructor(
    message: string,
    public code: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = 'QueueboneError';
  }
}

export class ConfigurationError extends QueueboneError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, 'CONFIGURATION_ERROR', options);
    this.name = 'ConfigurationError';
  }
}

export class InvalidStateError extends QueueboneError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, 'INVALID_STATE_ERROR', options);
    this.name = 'InvalidStateError';
  }
}

export class UnsupportedOperationError extends QueueboneError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, 'INVALID_STATE_ERROR', options);
    this.name = 'InvalidStateError';
  }
}

export class ConnectionError extends QueueboneError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, 'CONNECTION_ERROR', options);
    this.name = 'ConnectionError';
  }
}
