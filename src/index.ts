export type Resources = Readonly<Record<string, number>>;
export interface BudgetOptions {
  maxQueue?: number;
}
export interface AcquireOptions {
  signal?: AbortSignal;
}
export interface Lease {
  readonly resources: Resources;
  readonly released: boolean;
  release(): boolean;
}
export interface BudgetSnapshot {
  capacity: Record<string, number>;
  used: Record<string, number>;
  available: Record<string, number>;
  overcommitted: Record<string, number>;
  activeLeases: number;
  queued: number;
  closed: boolean;
}
export interface Budget {
  tryAcquire(resources: Resources): Lease | undefined;
  acquire(resources: Resources, options?: AcquireOptions): Promise<Lease>;
  resize(capacity: Resources): void;
  snapshot(): BudgetSnapshot;
  close(): void;
}
export class BudgetError extends Error {
  constructor(
    readonly code: 'closed' | 'capacity' | 'queue-full',
    message: string,
  ) {
    super(message);
    this.name = 'BudgetError';
  }
}
function integer(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new RangeError(`${name} must be a non-negative safe integer.`);
  return value;
}
function resources(value: Resources): Record<string, number> {
  if (
    !value ||
    typeof value !== 'object' ||
    (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
  ) {
    throw new TypeError('Resources must be a plain record.');
  }
  const keys = Reflect.ownKeys(value);
  if (keys.length === 0 || keys.length > 32) throw new RangeError('Expected 1..32 resources.');
  return Object.fromEntries(
    keys.map((key) => {
      if (typeof key !== 'string' || key.length === 0 || key.length > 128)
        throw new TypeError('Invalid resource name.');
      const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
      if (!descriptor.enumerable || !('value' in descriptor))
        throw new TypeError('Resources must be enumerable data properties.');
      return [key, integer(descriptor.value, key)];
    }),
  );
}
interface Waiter {
  request: Resources;
  resolve: (lease: Lease) => void;
  reject: (error: unknown) => void;
  cleanup: () => void;
}
export function createBudget(initial: Resources, options: BudgetOptions = {}): Budget {
  let capacity = resources(initial);
  const keys = Object.keys(capacity);
  const used = Object.fromEntries(keys.map((key) => [key, 0]));
  const queue: Waiter[] = [];
  const maximum = integer(options.maxQueue ?? 1000, 'maxQueue');
  let closed = false;
  let active = 0;
  const ensureOpen = () => {
    if (closed) throw new BudgetError('closed', 'Resource budget is closed.');
  };
  const possible = (request: Resources) =>
    Object.entries(request).every(([key, amount]) => amount <= capacity[key]!);
  const fits = (request: Resources) =>
    Object.entries(request).every(
      ([key, amount]) => amount === 0 || amount <= capacity[key]! - used[key]!,
    );
  const request = (input: Resources) => {
    const result = resources(input);
    if (!Object.values(result).some((amount) => amount > 0))
      throw new RangeError('A request must reserve at least one unit.');
    for (const key of Object.keys(result))
      if (!Object.hasOwn(capacity, key)) throw new TypeError(`Unknown resource: ${key}`);
    if (!possible(result))
      throw new BudgetError('capacity', 'Request exceeds current resource capacity.');
    return Object.freeze(result);
  };
  const drain = () => {
    while (queue.length) {
      const waiter = queue[0]!;
      if (!fits(waiter.request)) break;
      queue.shift();
      waiter.cleanup();
      waiter.resolve(admit(waiter.request));
    }
  };
  const admit = (value: Resources): Lease => {
    for (const [key, amount] of Object.entries(value)) used[key] = used[key]! + amount;
    active++;
    let released = false;
    return Object.freeze({
      resources: value,
      get released() {
        return released;
      },
      release() {
        if (released) return false;
        released = true;
        for (const [key, amount] of Object.entries(value)) used[key] = used[key]! - amount;
        active--;
        drain();
        return true;
      },
    });
  };
  return {
    tryAcquire(input) {
      ensureOpen();
      const value = request(input);
      return queue.length || !fits(value) ? undefined : admit(value);
    },
    async acquire(input, config = {}) {
      ensureOpen();
      const signal = config.signal;
      if (
        signal !== undefined &&
        (typeof signal.throwIfAborted !== 'function' ||
          typeof signal.addEventListener !== 'function' ||
          typeof signal.removeEventListener !== 'function')
      ) {
        throw new TypeError('Expected an AbortSignal.');
      }
      signal?.throwIfAborted();
      const value = request(input);
      if (!queue.length && fits(value)) return admit(value);
      if (queue.length === maximum)
        throw new BudgetError('queue-full', 'Resource wait queue is full.');
      return new Promise<Lease>((resolve, reject) => {
        const waiter: Waiter = {
          request: value,
          resolve,
          reject,
          cleanup: () => signal?.removeEventListener('abort', abort),
        };
        const abort = () => {
          const index = queue.indexOf(waiter);
          if (index < 0) return;
          queue.splice(index, 1);
          waiter.cleanup();
          reject(signal!.reason);
          drain();
        };
        queue.push(waiter);
        signal?.addEventListener('abort', abort, { once: true });
      });
    },
    resize(input) {
      ensureOpen();
      const next = resources(input);
      if (
        Object.keys(next).length !== keys.length ||
        keys.some((key) => !Object.hasOwn(next, key))
      ) {
        throw new TypeError('Resize must preserve exactly the original resource names.');
      }
      capacity = next;
      for (let index = queue.length - 1; index >= 0; index--) {
        const waiter = queue[index]!;
        if (!possible(waiter.request)) {
          queue.splice(index, 1);
          waiter.cleanup();
          waiter.reject(
            new BudgetError('capacity', 'Queued request exceeds resized resource capacity.'),
          );
        }
      }
      drain();
    },
    snapshot() {
      return {
        capacity: { ...capacity },
        used: { ...used },
        available: Object.fromEntries(
          keys.map((key) => [key, Math.max(0, capacity[key]! - used[key]!)]),
        ),
        overcommitted: Object.fromEntries(
          keys.map((key) => [key, Math.max(0, used[key]! - capacity[key]!)]),
        ),
        activeLeases: active,
        queued: queue.length,
        closed,
      };
    },
    close() {
      if (closed) return;
      closed = true;
      for (const waiter of queue.splice(0)) {
        waiter.cleanup();
        waiter.reject(new BudgetError('closed', 'Resource budget closed while waiting.'));
      }
    },
  };
}
export async function withBudget<T>(
  budget: Budget,
  resources: Resources,
  operation: (lease: Lease) => T | PromiseLike<T>,
  options: AcquireOptions = {},
): Promise<T> {
  if (typeof operation !== 'function') throw new TypeError('An operation callback is required.');
  const lease = await budget.acquire(resources, options);
  try {
    options.signal?.throwIfAborted();
    return await operation(lease);
  } finally {
    lease.release();
  }
}
