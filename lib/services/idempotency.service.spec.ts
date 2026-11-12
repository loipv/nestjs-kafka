import { IdempotencyService } from './idempotency.service';

const NS = 'default:g|t';

describe('IdempotencyService cleanup', () => {
  it('sweeps ALL expired entries in one cycle regardless of count', () => {
    jest.useFakeTimers();
    const svc = new IdempotencyService();
    const keyOf = (m: any) => m.headers!.k;

    for (let i = 0; i < 2000; i++) {
      svc.markProcessed({ headers: { k: `key-${i}` } } as any, keyOf, NS, 1000);
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
    expect(svc.isProcessed(m, byId, NS)).toBe(false);
    svc.markProcessed(m, byId, NS);
    expect(svc.isProcessed(m, byId, NS)).toBe(true);
  });

  it('never treats a message without a key as a duplicate', () => {
    const m = { headers: { 'idempotency-key': 'k' } } as any; // no `id` header
    svc.markProcessed(m, byId, NS);
    expect(svc.isProcessed(m, byId, NS)).toBe(false);
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
      timed.markProcessed(m, byId, NS, 1000);
      jest.advanceTimersByTime(1500);
      expect(timed.isProcessed(m, byId, NS)).toBe(false);
      timed.stopCleanup();
    } finally {
      jest.useRealTimers();
    }
  });

  it('filterDuplicates removes processed', () => {
    const a = { headers: { id: 'a' } } as any;
    const b = { headers: { id: 'b' } } as any;
    svc.markProcessed(a, byId, NS);
    expect(svc.filterDuplicates([a, b], byId, NS)).toEqual([b]);
  });

  it('does not treat the same key in another namespace as a duplicate', () => {
    const m = { headers: { id: 'k' } } as any;
    svc.markProcessed(m, byId, 'a:g|t1');
    expect(svc.isProcessed(m, byId, 'a:g|t2')).toBe(false);
    expect(svc.isProcessed(m, byId, 'a:g|t1')).toBe(true);
  });
});

describe('IdempotencyService size bound', () => {
  const byId = (m: any) => m.headers.id;
  const msg = (id: string) => ({ headers: { id } }) as any;

  it('evicts the oldest keys beyond idempotencyMaxKeys', () => {
    const svc = new IdempotencyService({ idempotencyMaxKeys: 2 });
    try {
      for (const id of ['k1', 'k2', 'k3']) svc.markProcessed(msg(id), byId, NS);
      expect(svc.isProcessed(msg('k1'), byId, NS)).toBe(false);
      expect(svc.isProcessed(msg('k2'), byId, NS)).toBe(true);
      expect(svc.isProcessed(msg('k3'), byId, NS)).toBe(true);
    } finally {
      svc.stopCleanup();
    }
  });

  it('re-marking a key refreshes its eviction order', () => {
    const svc = new IdempotencyService({ idempotencyMaxKeys: 2 });
    try {
      for (const id of ['k1', 'k2', 'k1', 'k3'])
        svc.markProcessed(msg(id), byId, NS);
      expect(svc.isProcessed(msg('k2'), byId, NS)).toBe(false);
      expect(svc.isProcessed(msg('k1'), byId, NS)).toBe(true);
    } finally {
      svc.stopCleanup();
    }
  });

  it.each([-1, 0, 1.5, Number.POSITIVE_INFINITY])(
    'rejects idempotencyMaxKeys %p',
    (idempotencyMaxKeys) => {
      expect(() => new IdempotencyService({ idempotencyMaxKeys })).toThrow(
        'idempotencyMaxKeys must be a positive integer',
      );
    },
  );

  it('defaults idempotencyMaxKeys to 100000', () => {
    const svc = new IdempotencyService();
    try {
      expect((svc as any).maxKeys).toBe(100000);
    } finally {
      svc.stopCleanup();
    }
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
