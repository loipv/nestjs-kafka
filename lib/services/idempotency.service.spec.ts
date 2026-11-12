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
  const byId = (m: any) => m.headers?.id?.toString();
  let svc: IdempotencyService;

  beforeEach(() => {
    svc = new IdempotencyService();
  });

  afterEach(() => {
    svc.stopCleanup();
  });

  it('dedupes by the extractor key', () => {
    const m = { headers: { id: Buffer.from('k') } } as any;
    expect(svc.isProcessed(m, byId)).toBe(false);
    svc.markProcessed(m, byId);
    expect(svc.isProcessed(m, byId)).toBe(true);
  });

  it('never treats a message without a key as a duplicate', () => {
    const m = { headers: { 'idempotency-key': 'k' } } as any; // no `id` header
    svc.markProcessed(m, byId);
    expect(svc.isProcessed(m, byId)).toBe(false);
  });

  it('requires an extractor', () => {
    const m = { headers: { 'idempotency-key': 'k' } } as any;
    expect(() =>
      // @ts-expect-error keyExtractor is required
      svc.markProcessed(m),
    ).toThrow(TypeError);
  });

  it('expires after TTL', () => {
    jest.useFakeTimers();
    try {
      const timed = new IdempotencyService();
      const m = { headers: { id: 'k2' } } as any;
      timed.markProcessed(m, byId, 1000);
      jest.advanceTimersByTime(1500);
      expect(timed.isProcessed(m, byId)).toBe(false);
      timed.stopCleanup();
    } finally {
      jest.useRealTimers();
    }
  });

  it('filterDuplicates removes processed', () => {
    const a = { headers: { id: 'a' } } as any;
    const b = { headers: { id: 'b' } } as any;
    svc.markProcessed(a, byId);
    expect(svc.filterDuplicates([a, b], byId)).toEqual([b]);
  });
});

describe('IdempotencyService timer', () => {
  it('cleanup interval does not keep the process alive (e.g. after a failed app init)', () => {
    const svc = new IdempotencyService();
    try {
      expect((svc as any).cleanupInterval.hasRef()).toBe(false);
    } finally {
      svc.stopCleanup();
    }
  });
});
