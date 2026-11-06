import {
  createBudget,
  withBudget,
  BudgetError,
  type Lease,
  type BudgetSnapshot,
} from '@lp-labs/resource-budget';
const budget = createBudget({ cpu: 2, bytes: 100 });
const lease: Lease = await budget.acquire({ cpu: 1 });
const result: string = await withBudget(budget, { bytes: 10 }, async () => 'ok');
const snapshot: BudgetSnapshot = budget.snapshot();
// @ts-expect-error reservations are readonly.
lease.resources.cpu = 10;
const error = new BudgetError('capacity', 'too large');
lease.release();
void [result, snapshot, error];
