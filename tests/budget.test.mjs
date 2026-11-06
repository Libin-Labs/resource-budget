import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createBudget, withBudget, BudgetError } from '../dist/esm/index.js';

test('multi-resource admission is atomic and never partly reserves a rejected request', () => {
  const budget = createBudget({ cpu: 2, bytes: 100 });
  const first = budget.tryAcquire({ cpu: 1, bytes: 90 });
  assert.ok(first);
  assert.equal(budget.tryAcquire({ cpu: 1, bytes: 20 }), undefined);
  assert.deepEqual(budget.snapshot().used, { cpu: 1, bytes: 90 });
  assert.equal(first.release(), true);
  assert.equal(first.release(), false);
  assert.equal(first.released, true);
  assert.deepEqual(budget.snapshot().used, { cpu: 0, bytes: 0 });
});
test('queued admission is FIFO and smaller requests cannot bypass a blocked head', async () => {
  const budget = createBudget({ units: 3 });
  const held = await budget.acquire({ units: 2 });
  const first = budget.acquire({ units: 3 });
  const second = budget.acquire({ units: 1 });
  assert.equal(budget.snapshot().queued, 2);
  assert.equal(budget.tryAcquire({ units: 1 }), undefined);
  held.release();
  const head = await first;
  assert.equal(budget.snapshot().queued, 1);
  head.release();
  const tail = await second;
  assert.equal(budget.snapshot().queued, 0);
  tail.release();
});
test('aborting a blocked head releases eligible waiters without leaking its reservation', async () => {
  const budget = createBudget({ units: 3 });
  const held = await budget.acquire({ units: 2 });
  const controller = new AbortController();
  const reason = new Error('cancel');
  const first = budget.acquire({ units: 3 }, { signal: controller.signal });
  const rejected = assert.rejects(first, (error) => error === reason);
  const second = budget.acquire({ units: 1 });
  controller.abort(reason);
  await rejected;
  const tail = await second;
  assert.equal(budget.snapshot().used.units, 3);
  tail.release();
  held.release();
  assert.equal(budget.snapshot().activeLeases, 0);
});
test('aborted acquisition does not reserve and abort after grant does not revoke a lease', async () => {
  const budget = createBudget({ units: 1 });
  const controller = new AbortController();
  controller.abort(new Error('before'));
  await assert.rejects(budget.acquire({ units: 1 }, { signal: controller.signal }), /before/);
  assert.equal(budget.snapshot().activeLeases, 0);
  const later = new AbortController();
  const lease = await budget.acquire({ units: 1 }, { signal: later.signal });
  later.abort();
  assert.equal(lease.released, false);
  assert.equal(budget.snapshot().used.units, 1);
  lease.release();
});
test('queue capacity counts only waiting requests and rejects explicitly', async () => {
  const budget = createBudget({ units: 1 }, { maxQueue: 1 });
  const lease = await budget.acquire({ units: 1 });
  const pending = budget.acquire({ units: 1 });
  await assert.rejects(
    budget.acquire({ units: 1 }),
    (error) => error instanceof BudgetError && error.code === 'queue-full',
  );
  lease.release();
  (await pending).release();
  const noQueue = createBudget({ units: 1 }, { maxQueue: 0 });
  const hold = await noQueue.acquire({ units: 1 });
  await assert.rejects(noQueue.acquire({ units: 1 }), /queue is full/);
  hold.release();
});
test('resizing up admits waiters and resizing down never revokes active work', async () => {
  const budget = createBudget({ units: 2 });
  const held = await budget.acquire({ units: 2 });
  const wait = budget.acquire({ units: 1 });
  budget.resize({ units: 3 });
  const second = await wait;
  budget.resize({ units: 1 });
  assert.equal(budget.snapshot().overcommitted.units, 2);
  assert.equal(budget.snapshot().available.units, 0);
  assert.equal(budget.tryAcquire({ units: 1 }), undefined);
  held.release();
  assert.equal(budget.snapshot().used.units, 1);
  second.release();
  assert.equal(budget.snapshot().overcommitted.units, 0);
});
test('resize rejects now-impossible waiters and preserves eligible FIFO order', async () => {
  const budget = createBudget({ units: 3 });
  const held = await budget.acquire({ units: 3 });
  const large = budget.acquire({ units: 3 });
  const rejected = assert.rejects(large, (error) => error.code === 'capacity');
  const small = budget.acquire({ units: 1 });
  budget.resize({ units: 1 });
  await rejected;
  assert.equal(budget.snapshot().queued, 1);
  held.release();
  (await small).release();
});
test('zero weights do not block unrelated work during overcommit', async () => {
  const budget = createBudget({ cpu: 1, bytes: 10 });
  const held = await budget.acquire({ bytes: 10 });
  budget.resize({ cpu: 1, bytes: 1 });
  const other = budget.tryAcquire({ cpu: 1, bytes: 0 });
  assert.ok(other);
  other.release();
  held.release();
});
test('close rejects all waiters but active leases can still release safely', async () => {
  const budget = createBudget({ units: 1 });
  const held = await budget.acquire({ units: 1 });
  const pending = budget.acquire({ units: 1 });
  const rejected = assert.rejects(pending, (error) => error.code === 'closed');
  budget.close();
  budget.close();
  await rejected;
  assert.throws(() => budget.tryAcquire({ units: 1 }), BudgetError);
  await assert.rejects(budget.acquire({ units: 1 }), BudgetError);
  assert.throws(() => budget.resize({ units: 2 }), BudgetError);
  held.release();
  assert.equal(budget.snapshot().activeLeases, 0);
});
test('withBudget releases on success, callback rejection and pre-callback abort', async () => {
  const budget = createBudget({ units: 1 });
  assert.equal(await withBudget(budget, { units: 1 }, (lease) => lease.resources.units + 1), 2);
  const reason = new Error('operation');
  await assert.rejects(
    withBudget(budget, { units: 1 }, async () => {
      throw reason;
    }),
    (error) => error === reason,
  );
  const controller = new AbortController();
  const run = withBudget(
    budget,
    { units: 1 },
    () => {
      throw new Error('must not run');
    },
    { signal: controller.signal },
  );
  controller.abort(reason);
  await assert.rejects(run, (error) => error === reason);
  assert.equal(budget.snapshot().used.units, 0);
});
test('requests and snapshots do not retain mutable caller objects', async () => {
  const capacity = { units: 2 };
  const budget = createBudget(capacity);
  capacity.units = 999;
  const request = { units: 1 };
  const held = await budget.acquire(request);
  request.units = 999;
  const snapshot = budget.snapshot();
  snapshot.used.units = 999;
  assert.equal(budget.snapshot().used.units, 1);
  assert.equal(budget.snapshot().capacity.units, 2);
  assert.throws(() => (held.resources.units = 99), TypeError);
  held.release();
});
test('own-key handling supports prototype-like resource names without invoking accessors', () => {
  const budget = createBudget(JSON.parse('{"__proto__":2,"constructor":1}'));
  const held = budget.tryAcquire(JSON.parse('{"__proto__":1}'));
  assert.equal(budget.snapshot().used.__proto__, 1);
  held.release();
  let invoked = false;
  const invalid = Object.defineProperty({}, 'units', {
    enumerable: true,
    get() {
      invoked = true;
      return 1;
    },
  });
  assert.throws(() => createBudget(invalid), TypeError);
  assert.equal(invoked, false);
});
test('invalid resource maps and impossible requests fail without queueing', async () => {
  for (const value of [-1, NaN, Infinity, 1.5])
    assert.throws(() => createBudget({ units: value }), RangeError);
  assert.throws(() => createBudget({}), RangeError);
  const budget = createBudget({ units: 1 });
  assert.throws(() => budget.tryAcquire({ unknown: 1 }), TypeError);
  assert.throws(() => budget.tryAcquire({ units: 0 }), RangeError);
  await assert.rejects(budget.acquire({ units: 2 }), (error) => error.code === 'capacity');
  assert.throws(() => budget.resize({ other: 1 }), TypeError);
  await assert.rejects(budget.acquire({ units: 1 }, { signal: {} }), TypeError);
  assert.equal(budget.snapshot().queued, 0);
});
test('many queued operations preserve caps and return every reservation', async () => {
  const budget = createBudget({ cpu: 3, bytes: 10 });
  let peak = 0;
  await Promise.all(
    Array.from({ length: 250 }, (_, index) =>
      withBudget(budget, { cpu: 1, bytes: 3 }, async () => {
        const state = budget.snapshot();
        peak = Math.max(peak, state.used.cpu);
        assert.ok(state.used.cpu <= 3);
        assert.ok(state.used.bytes <= 10);
        await Promise.resolve();
        return index;
      }),
    ),
  );
  assert.equal(peak, 3);
  assert.equal(budget.snapshot().activeLeases, 0);
  assert.equal(budget.snapshot().queued, 0);
});
test('CommonJS supports atomic leases and automatic release', async () => {
  const api = createRequire(import.meta.url)('../dist/cjs/index.js');
  const budget = api.createBudget({ units: 1 });
  assert.equal(await api.withBudget(budget, { units: 1 }, () => 'ok'), 'ok');
  assert.equal(budget.snapshot().used.units, 0);
});
