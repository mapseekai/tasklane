import { integer, RuntimeError } from '../errors.js';
import type { RuntimeBudgets, TaskBudget, Priority } from '../types.js';

const keys = ['inputBytes', 'scratchBytes', 'outputBytes', 'cacheBytes', 'residentBytes'] as const;
export const ZERO_BUDGET: Required<RuntimeBudgets> = {
  inputBytes: 0,
  scratchBytes: 0,
  outputBytes: 0,
  cacheBytes: 0,
  residentBytes: 0,
};

/** Reservations are accounting, not operating-system or GPU memory guarantees. */
export class BudgetLedger {
  readonly used = { ...ZERO_BUDGET };
  readonly peak = { ...ZERO_BUDGET };
  readonly limits: Required<RuntimeBudgets>;
  readonly nonInteractive = { ...ZERO_BUDGET };
  readonly protected: Required<RuntimeBudgets>;
  constructor(limits: RuntimeBudgets, reserve: Partial<RuntimeBudgets> = {}) {
    this.limits = { ...limits, residentBytes: limits.residentBytes ?? 0 };
    this.protected = { ...ZERO_BUDGET };
    for (const key of keys) {
      integer(this.limits[key], key);
      this.protected[key] = integer(reserve[key] ?? 0, `interactive ${key}`);
    }
    this.validate(this.protected);
  }
  validate(cost: Partial<RuntimeBudgets>, priority: Priority = 'interactive'): void {
    for (const key of keys) {
      const value = cost[key] ?? 0;
      integer(value, key);
      const limit = this.limits[key] - (priority === 'interactive' ? 0 : this.protected[key]);
      if (value > limit) {
        throw new RuntimeError(
          'BUDGET_EXCEEDED',
          `${key}=${value} exceeds admission limit ${limit}`,
        );
      }
    }
  }
  available(key: keyof RuntimeBudgets, priority: Priority = 'interactive'): number {
    const total = this.limits[key] - this.used[key];
    return priority === 'interactive'
      ? total
      : Math.min(total, this.limits[key] - this.protected[key] - this.nonInteractive[key]);
  }
  fits(cost: Partial<RuntimeBudgets>, priority: Priority = 'interactive'): boolean {
    return keys.every((key) => (cost[key] ?? 0) <= this.available(key, priority));
  }
  reserve(cost: Partial<RuntimeBudgets>, priority: Priority = 'foreground'): () => void {
    this.validate(cost, priority);
    if (!this.fits(cost, priority))
      throw new RuntimeError('BUDGET_EXCEEDED', 'Reservation is not available');
    const held = { ...cost };
    for (const key of keys) {
      this.used[key] += held[key] ?? 0;
      if (priority !== 'interactive') this.nonInteractive[key] += held[key] ?? 0;
      this.peak[key] = Math.max(this.peak[key], this.used[key]);
    }
    let released = false;
    return () => {
      if (released) return;
      released = true;
      for (const key of keys) {
        this.used[key] -= held[key] ?? 0;
        if (priority !== 'interactive') this.nonInteractive[key] -= held[key] ?? 0;
      }
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
