import { IdempotencyService } from './idempotency.service';

describe('IdempotencyService cleanup', () => {
  it('sweeps ALL expired entries in one cycle regardless of count', () => {
    jest.useFakeTimers();
    const svc = new IdempotencyService();
    const keyOf = (m: any) => m.headers!.k;

    for (let i = 0; i < 2000; i++) {
      svc.markProcessed({ headers: { k: `key-${i}` } } as any, keyOf, 1000);
    }

    jest.advanceTimersByTime(61_000); // past TTL + one cleanup interval

    expect((svc as any).processedKeys.size).toBe(0);
    svc.stopCleanup();
    jest.useRealTimers();
  });
});

describe('dedupe semantics', () => {
  it('dedupes by idempotency-key header (Buffer or string)', () => {
    const svc = new IdempotencyService();
    const msg = { headers: { 'idempotency-key': Buffer.from('k1') } } as any;
    expect(svc.isProcessed(msg)).toBe(false);
    svc.markProcessed(msg);
    expect(svc.isProcessed(msg)).toBe(true);
    expect(
      svc.isProcessed({ headers: { 'idempotency-key': 'k1' } } as any),
    ).toBe(true);
  });

  it('expires after TTL', () => {
    jest.useFakeTimers();
    const svc = new IdempotencyService();
    const msg = { headers: { 'idempotency-key': 'k2' } } as any;
    svc.markProcessed(msg, undefined, 1000);
    jest.advanceTimersByTime(1500);
    expect(svc.isProcessed(msg)).toBe(false);
    svc.stopCleanup();
    jest.useRealTimers();
  });

  it('filterDuplicates removes processed', () => {
    const svc = new IdempotencyService();
    const a = { headers: { 'idempotency-key': 'a' } } as any;
    const b = { headers: { 'idempotency-key': 'b' } } as any;
    svc.markProcessed(a);
    expect(svc.filterDuplicates([a, b])).toEqual([b]);
  });
});
