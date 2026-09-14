import { ConsumerRegistryService } from './consumer-registry.service';
import { BatchProcessorService } from './batch-processor.service';

describe('ConsumerRegistryService.mergeWithDefaults', () => {
  const makeRegistry = (moduleOptions?: any) =>
    new ConsumerRegistryService(
      {} as any, {} as any, {} as any, {} as any, {} as any, {} as any,
      undefined, moduleOptions,
    );

  it('field-merges retry: decorator retries + default skip flag survive', () => {
    const registry = makeRegistry({
      retry: { retries: 3, skipMessageOnMaxRetries: true },
    });
    const merged = (registry as any).mergeWithDefaults({
      retry: { retries: 5 },
    }) as any;
    expect(merged.retry).toEqual({ retries: 5, skipMessageOnMaxRetries: true });
  });

  it('uses module default retry when decorator has none', () => {
    const registry = makeRegistry({ retry: { retries: 7 } });
    const merged = (registry as any).mergeWithDefaults({}) as any;
    expect(merged.retry).toEqual({ retries: 7 });
  });

  it('merges auto-create topic sizing defaults per field', () => {
    const registry = makeRegistry({
      autoCreateTopicPartitions: 3,
      autoCreateTopicReplicationFactor: 3,
    });
    const merged = (registry as any).mergeWithDefaults({}) as any;
    expect(merged.autoCreateTopicPartitions).toBe(3);
    expect(merged.autoCreateTopicReplicationFactor).toBe(3);
  });
});

describe('retry logic', () => {
  const mkMsg = () => ({ offset: '1' }) as any;
  const makeRegistry = () =>
    new ConsumerRegistryService(
      {} as any, {} as any,
      { stopCleanup: jest.fn() } as any,
      { register: jest.fn(), setTopics: jest.fn() } as any,
      { handleFailure: jest.fn() } as any,
      { registerOriginalGroupId: jest.fn(), gracefulShutdown: jest.fn().mockResolvedValue(undefined) } as any,
      undefined, undefined,
    );

  it('evaluateRetry: retry with capped delay', () => {
    const r = makeRegistry();
    expect((r as any).evaluateRetry(1, {})).toEqual({ action: 'retry', delayMs: 1000 });
    expect((r as any).evaluateRetry(1, { retry: { initialRetryTime: 60000 } }))
      .toEqual({ action: 'retry', delayMs: 30000 });
  });

  it('evaluateRetry: default (skip=false) retries forever past maxRetries; skip=true completes', () => {
    const r = makeRegistry();
    expect((r as any).evaluateRetry(4, {})).toEqual({ action: 'retry', delayMs: 8000 }); // past maxRetries=3, still retrying
    expect((r as any).evaluateRetry(4, { retry: { skipMessageOnMaxRetries: true } }))
      .toEqual({ action: 'complete' });
  });

  it('runWithRetry re-invokes in-process until success', async () => {
    const r = makeRegistry();
    const attempts = jest.fn()
      .mockRejectedValueOnce(new Error('1'))
      .mockRejectedValueOnce(new Error('2'))
      .mockResolvedValue(undefined);
    await (r as any).runWithRetry(
      attempts, mkMsg(),
      { topic: 't', connection: 'default', options: { retry: { retries: 3, initialRetryTime: 1 } } } as any,
      0,
    );
    expect(attempts).toHaveBeenCalledTimes(3);
  });

  it('runWithRetry succeeds after retries are exceeded (infinite retry, no crash)', async () => {
    const r = makeRegistry();
    const attempts = jest.fn()
      .mockRejectedValueOnce(new Error('1'))
      .mockRejectedValueOnce(new Error('2'))
      .mockRejectedValueOnce(new Error('3'))
      .mockRejectedValueOnce(new Error('4'))
      .mockResolvedValue(undefined);
    await (r as any).runWithRetry(
      attempts, mkMsg(),
      { topic: 't', connection: 'default', options: { retry: { retries: 2, initialRetryTime: 1 } } } as any,
      0,
    );
    expect(attempts).toHaveBeenCalledTimes(5); // succeeded 2 attempts PAST maxRetries=2
  });

  it('DLQ send failure propagates (infra crash path → auto-restart)', async () => {
    const r = makeRegistry();
    (r as any).dlqService = { handleFailure: jest.fn().mockRejectedValue(new Error('dlq down')) };
    const attempts = jest.fn().mockRejectedValue(new Error('x'));
    await expect((r as any).runWithRetry(
      attempts, mkMsg(),
      { topic: 't', connection: 'default', options: { dlq: { topic: 'd' }, retry: { retries: 0, initialRetryTime: 1 } } } as any,
      0,
    )).rejects.toThrow('dlq down');
  });

  it('shutdown aborts the retry loop — rethrows once, no hot-loop, offset not committed', async () => {
    const r = makeRegistry();
    const attempts = jest.fn().mockRejectedValue(new Error('x'));
    const p = (r as any).runWithRetry(
      attempts, mkMsg(),
      { topic: 't', connection: 'default', options: { retry: { retries: 50, initialRetryTime: 60_000 } } } as any,
      0,
    );
    await (r as any).gracefulShutdown(); // resolves pending sleep
    await expect(p).rejects.toThrow('x'); // one-shot rethrow → message redelivered next boot
    expect(attempts).toHaveBeenCalledTimes(1); // no hot-loop
  });
});
