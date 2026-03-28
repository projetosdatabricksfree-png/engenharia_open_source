/**
 * In-memory TTL cache using private class fields and ES2023+ patterns.
 * Generic — works with any JSON-serializable value.
 */

interface CacheEntry<T> {
  readonly value: T;
  readonly expiresAt: number;
}

export class TtlCache<T> {
  readonly #store = new Map<string, CacheEntry<T>>();
  readonly #ttlMs: number;

  constructor(ttlMs = 60_000) {
    this.#ttlMs = ttlMs;
  }

  /** Returns cached value if still fresh, otherwise undefined. */
  get(key: string): T | undefined {
    const entry = this.#store.get(key);
    if (!entry) return undefined;
    if (Date.now() > entry.expiresAt) {
      this.#store.delete(key);
      return undefined;
    }
    return entry.value;
  }

  set(key: string, value: T): void {
    this.#store.set(key, { value, expiresAt: Date.now() + this.#ttlMs });
  }

  invalidate(key: string): void {
    this.#store.delete(key);
  }

  clear(): void {
    this.#store.clear();
  }

  get size() { return this.#store.size; }
}
