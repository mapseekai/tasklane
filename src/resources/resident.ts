import { integer, RuntimeError } from '../errors.js';
import type { ResourceLease, Priority } from '../types.js';
import type { BudgetLedger } from './budget.js';

/** Credits only: the owner must dispose data before returning or shrinking its reservation. */
export class ResidentLease implements ResourceLease {
  private held: number;
  private disposed = false;
  private returnCredits: () => void;
  constructor(
    private readonly ledger: BudgetLedger,
    bytes: number,
    private onRelease: () => void,
    private onResize: () => void,
    private readonly priority: Priority = 'foreground',
  ) {
    integer(bytes, 'resident bytes');
    this.returnCredits = ledger.reserve({ residentBytes: bytes }, priority);
    this.held = bytes;
  }
  get bytes(): number {
    return this.held;
  }
  get released(): boolean {
    return this.disposed;
  }
  resize(bytes: number): void {
    if (this.disposed) throw new RuntimeError('CLOSED', 'Resource lease has been released');
    integer(bytes, 'resident bytes');
    this.ledger.validate({ residentBytes: bytes }, this.priority);
    if (!this.ledger.fits({ residentBytes: bytes - this.held }, this.priority))
      throw new RuntimeError('BUDGET_EXCEEDED', 'Resident reservation is not available');
    this.returnCredits();
    this.returnCredits = this.ledger.reserve({ residentBytes: bytes }, this.priority);
    this.held = bytes;
    this.onResize();
  }
  release(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.returnCredits();
    this.held = 0;
    const released = this.onRelease;
    this.onRelease = () => {};
    this.onResize = () => {};
    released();
  }
}
