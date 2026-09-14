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
