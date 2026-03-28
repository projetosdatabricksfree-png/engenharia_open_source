/**
 * Custom error hierarchy for the BrasileirãoPRO API client.
 * Follows ES2023+ class patterns with private fields.
 */

export class ApiError extends Error {
  readonly #statusCode: number;
  readonly #endpoint: string;

  constructor(message: string, statusCode: number, endpoint: string) {
    super(message);
    this.name = 'ApiError';
    this.#statusCode = statusCode;
    this.#endpoint = endpoint;
  }

  get statusCode() { return this.#statusCode; }
  get endpoint() { return this.#endpoint; }

  get isNotFound() { return this.#statusCode === 404; }
  get isServerError() { return this.#statusCode >= 500; }
  get isNetworkError() { return this.#statusCode === 0; }
}

export class NetworkError extends ApiError {
  constructor(endpoint: string, cause?: unknown) {
    super(`Network unreachable — is the API running on :8000?`, 0, endpoint);
    this.name = 'NetworkError';
    this.cause = cause;
  }
}

export class TimeoutError extends ApiError {
  constructor(endpoint: string, timeoutMs: number) {
    super(`Request timed out after ${timeoutMs}ms`, 408, endpoint);
    this.name = 'TimeoutError';
  }
}
