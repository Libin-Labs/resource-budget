import { createBudget, withBudget, type Resources } from '@lp-labs/resource-budget';
const resources: Resources = { bytes: 10 };
const result: Promise<number> = withBudget(createBudget(resources), resources, () => 1);
void result;
