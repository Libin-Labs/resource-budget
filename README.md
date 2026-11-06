# @lp-labs/resource-budget

Coordinate cooperative CPU, memory, network or domain-specific quotas with atomic multi-resource reservations. A request reserves all its requested units together or waits without reserving any.

- Zero runtime or peer dependencies; no imports from sibling packages.
- ESM, CommonJS and TypeScript declarations.
- Bounded FIFO admission, abortable waits, idempotent leases and explicit shutdown.
- Capacity resizing with overcommit diagnostics rather than revoking running work.
- Framework-independent; no timers, global listeners or browser globals at import.

## Installation

```sh
npm install @lp-labs/resource-budget
```

Requires Node.js 18+ or a modern browser with an ESM-aware bundler. Abortable acquisition uses the platform's `AbortSignal`.

## Quickstart

```js
import { createBudget, withBudget } from '@lp-labs/resource-budget';

const budget = createBudget({ cpu: 2, bytes: 100 }, { maxQueue: 100 });
const results = await Promise.all([10, 20, 30].map((value) =>
  withBudget(budget, { cpu: 1, bytes: 40 }, async () => {
    await Promise.resolve();
    return value * 2;
  }),
));
console.log(results); // [20, 40, 60]
console.log(budget.snapshot().used); // { cpu: 0, bytes: 0 }
budget.close();
```

CommonJS: `const { createBudget, withBudget } = require('@lp-labs/resource-budget')`.

## Admission model

Resource units are application-defined non-negative safe integers. A `bytes` budget does not measure the JavaScript heap; a `cpu` budget does not enforce OS scheduling. Every participating feature must declare realistic usage and honor its reservation. This is **process-local cooperative accounting**, not a sandbox, distributed semaphore or memory allocator.

Resource names are fixed at creation. A budget accepts 1..32 own enumerable string-keyed data properties, names 1..128 UTF-16 units. Plain/null-prototype records are supported; accessors and symbol keys are rejected. Prototype-like names remain ordinary own fields.

Requests may reserve a subset of resources and must contain at least one positive unit. Zero entries are permitted but do not prevent unrelated work while that resource is overcommitted. Unknown resources throw `TypeError`. Requests above current capacity reject immediately rather than wait for a future resize.

An admitted reservation counts as used before its lease promise resolves. Releasing it returns every reserved resource exactly once.

## API

Runtime exports: `createBudget`, `withBudget`, `BudgetError`.

Type exports: `Resources`, `BudgetOptions`, `AcquireOptions`, `Lease`, `BudgetSnapshot`, `Budget`.

### createBudget(capacity, options?)

`maxQueue` defaults to 1000 and must be a non-negative safe integer. It counts only waiting requests; zero disables waiting but allows immediate admission.

| Method | Behavior |
|---|---|
| `tryAcquire(resources)` | Return a lease immediately, or `undefined` if temporarily unavailable or older requests are waiting. Throws on invalid/impossible/closed requests. |
| `acquire(resources, { signal }?)` | Return a promise for an immediate or queued lease. Reject on invalid input, capacity, queue overflow, abort or close. |
| `resize(capacity)` | Replace capacities while preserving exactly the original resource names. |
| `snapshot()` | Return independent accounting and queue diagnostics. |
| `close()` | Reject all waiting requests and prevent future admission/resizing. Idempotent. |

`acquire` errors reject its promise; other method validation errors throw synchronously.

### Lease

A lease exposes frozen `resources`, a live readonly `released` getter and `release(): boolean`. The first release returns true; repeats return false. Keep the lease until actual resource use is finished, not merely until an operation starts.

```js
import { createBudget } from '@lp-labs/resource-budget';
const budget = createBudget({ connections: 1 });
const lease = await budget.acquire({ connections: 1 });
try {
  console.log(lease.resources.connections); // 1
} finally {
  lease.release();
}
```

There is no finalizer or lease timeout. Forgetting to release can block all queued work indefinitely. Releasing early or making undeclared allocations invalidates accounting guarantees.

### withBudget(budget, resources, operation, options?)

Acquire a lease, check cancellation before calling `operation(lease)`, await its result and release in `finally`. The callback may be synchronous or asynchronous. Operation exceptions propagate.

```js
import { createBudget, withBudget } from '@lp-labs/resource-budget';
const budget = createBudget({ network: 1 });
const controller = new AbortController();
const value = await withBudget(budget, { network: 1 }, async () => {
  controller.signal.throwIfAborted();
  return 'completed';
}, { signal: controller.signal });
console.log(value);
```

After admission, abort does **not** forcibly revoke a lease or cancel the operation. Pass the same signal into fetch/storage/worker APIs and await cleanup before returning. `withBudget` releases only when the callback settles.

## Fairness, resizing and shutdown

Waiting requests are strictly FIFO. A large blocked request prevents smaller later requests from bypassing it, even if they would fit. This avoids starvation by opportunistic arrivals at the cost of head-of-line blocking. Abort removes a waiter and immediately attempts admission of the new head.

Increasing capacity can admit waiters immediately. Decreasing below current usage leaves active leases valid and reports overcommit. Queued requests now larger than capacity reject with a capacity error; eligible remaining requests keep their FIFO order. Other work can use unaffected resources.

`close()` rejects waiters but does not revoke active leases or await active operations. Active leases can still release safely after closure. Snapshot accounting remains readable. There is no reopening.

`snapshot()` returns:

- independent `capacity` and `used` records;
- `available` as `max(0, capacity - used)`;
- `overcommitted` as `max(0, used - capacity)`;
- `activeLeases`, `queued`, `closed`.

Use these values for application pressure notifications or dashboards. This release intentionally has no observer callback, polling timer, process-memory sensor or automatic capacity adaptation.

## Cancellation and errors

Already-aborted waits reject with the signal's reason without reserving anything. Abort after an already-granted lease does not reclaim it; callers that await raw `acquire` must release even when cancellation arrives in the same turn. `withBudget` handles that pre-callback race.

Queued abort listeners are detached on grant, cancellation, resize rejection and shutdown. Signals must be native-compatible AbortSignals, not arbitrary event emitters.

`BudgetError` has code `'closed'`, `'capacity'` or `'queue-full'`. Invalid map shapes/names/signals throw `TypeError`; unsafe quantities and queue sizes throw `RangeError`. Temporarily unavailable `tryAcquire` returns `undefined` as its documented nonblocking result, not a swallowed error.

## Performance and limits

Admission checks at most 32 resources. Waiting uses an array: head removal/abort/resizing can cost O(queue length). Queue bounds prevent unbounded waiter accumulation but not arbitrary caller closure sizes. Reservations hold only scalar records, not the underlying resources.

No priorities, preemption, deadlines, distributed coordination or durable persistence are provided. Avoid cyclic acquisition of multiple budgets; use one multi-resource reservation instead. Nested acquisition can deadlock if an operation holds resources while waiting for more from the same exhausted budget.

This independently authored initial release verifies FIFO, resize/abort races and generated workloads; it does not claim hard real-time or OS-level enforcement.

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
