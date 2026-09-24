import { integer, RuntimeError } from '../errors.js';
import type { RuntimeBudgets, TaskBudget } from '../types.js';

const keys = ['inputBytes', 'scratchBytes', 'outputBytes', 'cacheBytes'] as const;
export const ZERO_BUDGET: RuntimeBudgets = {
  inputBytes: 0,
  scratchBytes: 0,
  outputBytes: 0,
  cacheBytes: 0,
};

/** Reservations are accounting, not operating-system or GPU memory guarantees. */
export class BudgetLedger {
  readonly used = { ...ZERO_BUDGET };
  readonly peak = { ...ZERO_BUDGET };
  readonly limits: RuntimeBudgets;
  constructor(limits: RuntimeBudgets) {
    for (const key of keys) integer(limits[key], key);
    this.limits = { ...limits };
  }
  validate(cost: Partial<RuntimeBudgets>): void {
    for (const key of keys) {
      const value = cost[key] ?? 0;
      integer(value, key);
      if (value > this.limits[key]) {
        throw new RuntimeError(
          'BUDGET_EXCEEDED',
          `${key}=${value} exceeds limit ${this.limits[key]}`,
        );
      }
    }
  }
  fits(cost: Partial<RuntimeBudgets>): boolean {
    return keys.every((key) => this.used[key] + (cost[key] ?? 0) <= this.limits[key]);
  }
  reserve(cost: Partial<RuntimeBudgets>): () => void {
    this.validate(cost);
    if (!this.fits(cost)) throw new RuntimeError('BUDGET_EXCEEDED', 'Reservation is not available');
    const held = { ...cost };
    for (const key of keys) {
      this.used[key] += held[key] ?? 0;
      this.peak[key] = Math.max(this.peak[key], this.used[key]);
    }
    let released = false;
    return () => {
      if (released) return;
      released = true;
      for (const key of keys) this.used[key] -= held[key] ?? 0;
    };
  }
}

export function validateTaskBudget(budget: TaskBudget): TaskBudget {
  return {
    inputBytes: integer(budget.inputBytes, 'inputBytes'),
    scratchBytes: integer(budget.scratchBytes, 'scratchBytes'),
    outputBytes: integer(budget.outputBytes, 'outputBytes'),
  };
}
