import { IdempotencyService } from './idempotency.service';

describe('IdempotencyService cleanup', () => {
  it('sweeps ALL expired entries in one cycle regardless of count', () => {
    jest.useFakeTimers();
    const svc = new IdempotencyService();
    const keyOf = (m: any) => m.headers!.k as any;

    for (let i = 0; i < 2000; i++) {
      svc.markProcessed({ headers: { k: `key-${i}` } } as any, keyOf, 1000);
    }

    jest.advanceTimersByTime(61_000); // past TTL + one cleanup interval

    expect((svc as any).processedKeys.size).toBe(0);
    svc.stopCleanup();
    jest.useRealTimers();
  });
});
