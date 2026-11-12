import { ConsumerRegistryService } from './consumer-registry.service';
import { BatchProcessorService } from './batch-processor.service';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript', () => ({
  KafkaJS: {
    Kafka: jest.fn(),
    logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 },
  },
}));

describe('ConsumerRegistryService.mergeWithDefaults', () => {
  const makeRegistry = (moduleOptions?: any) =>
    new ConsumerRegistryService(
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      undefined,
      moduleOptions,
    );

  it('field-merges retry: decorator retries + default skip flag survive', () => {
    const registry = makeRegistry({
      retry: { retries: 3, skipMessageOnMaxRetries: true },
    });
    const merged = (registry as any).mergeWithDefaults({
      retry: { retries: 5 },
    });
    expect(merged.retry).toEqual({ retries: 5, skipMessageOnMaxRetries: true });
  });

  it('uses module default retry when decorator has none', () => {
    const registry = makeRegistry({ retry: { retries: 7 } });
    const merged = (registry as any).mergeWithDefaults({});
    expect(merged.retry).toEqual({ retries: 7 });
  });

  it('merges auto-create topic sizing defaults per field', () => {
    const registry = makeRegistry({
      autoCreateTopicPartitions: 3,
      autoCreateTopicReplicationFactor: 3,
    });
    const merged = (registry as any).mergeWithDefaults({});
    expect(merged.autoCreateTopicPartitions).toBe(3);
    expect(merged.autoCreateTopicReplicationFactor).toBe(3);
  });
});

describe('retry logic', () => {
  const mkMsg = () => ({ offset: '1' }) as any;
  const makeRegistry = () =>
    new ConsumerRegistryService(
      {} as any,
      {} as any,
      { stopCleanup: jest.fn() } as any,
      { handleFailure: jest.fn() } as any,
      undefined,
      undefined,
    );

  it('evaluateRetry: retry with capped delay', () => {
    const r = makeRegistry();
    expect((r as any).evaluateRetry(1, {})).toEqual({
      action: 'retry',
      delayMs: 1000,
    });
    expect(
      (r as any).evaluateRetry(1, { retry: { initialRetryTime: 60000 } }),
    ).toEqual({ action: 'retry', delayMs: 30000 });
  });

  it('evaluateRetry: default (skip=false) retries forever past maxRetries; skip=true completes', () => {
    const r = makeRegistry();
    expect((r as any).evaluateRetry(4, {})).toEqual({
      action: 'retry',
      delayMs: 8000,
    }); // past maxRetries=3, still retrying
    expect(
      (r as any).evaluateRetry(4, { retry: { skipMessageOnMaxRetries: true } }),
    ).toEqual({ action: 'complete' });
  });

  it('runWithRetry re-invokes in-process until success', async () => {
    const r = makeRegistry();
    const attempts = jest
      .fn()
      .mockRejectedValueOnce(new Error('1'))
      .mockRejectedValueOnce(new Error('2'))
      .mockResolvedValue(undefined);
    await (r as any).runWithRetry(
      attempts,
      mkMsg(),
      {
        topic: 't',
        connection: 'default',
        options: { retry: { retries: 3, initialRetryTime: 1 } },
      } as any,
      0,
    );
    expect(attempts).toHaveBeenCalledTimes(3);
  });

  it('runWithRetry succeeds after retries are exceeded (infinite retry, no crash)', async () => {
    const r = makeRegistry();
    const attempts = jest
      .fn()
      .mockRejectedValueOnce(new Error('1'))
      .mockRejectedValueOnce(new Error('2'))
      .mockRejectedValueOnce(new Error('3'))
      .mockRejectedValueOnce(new Error('4'))
      .mockResolvedValue(undefined);
    await (r as any).runWithRetry(
      attempts,
      mkMsg(),
      {
        topic: 't',
        connection: 'default',
        options: { retry: { retries: 2, initialRetryTime: 1 } },
      } as any,
      0,
    );
    expect(attempts).toHaveBeenCalledTimes(5); // succeeded 2 attempts PAST maxRetries=2
  });

  it('DLQ send failure propagates (the client redelivers the message)', async () => {
    const r = makeRegistry();
    (r as any).dlqService = {
      handleFailure: jest.fn().mockRejectedValue(new Error('dlq down')),
    };
    const attempts = jest.fn().mockRejectedValue(new Error('x'));
    await expect(
      (r as any).runWithRetry(
        attempts,
        mkMsg(),
        {
          topic: 't',
          connection: 'default',
          options: {
            dlq: { topic: 'd' },
            retry: { retries: 0, initialRetryTime: 1 },
          },
        } as any,
        0,
      ),
    ).rejects.toThrow('dlq down');
  });

  it('shutdown aborts the retry loop — rethrows once, no hot-loop, offset not committed', async () => {
    const r = makeRegistry();
    const attempts = jest.fn().mockRejectedValue(new Error('x'));
    const p = (r as any).runWithRetry(
      attempts,
      mkMsg(),
      {
        topic: 't',
        connection: 'default',
        options: { retry: { retries: 50, initialRetryTime: 60_000 } },
      } as any,
      0,
    );
    await (r as any).gracefulShutdown(); // resolves pending sleep
    await expect(p).rejects.toThrow('x'); // one-shot rethrow → message redelivered next boot
    expect(attempts).toHaveBeenCalledTimes(1); // no hot-loop
  });
});

describe('consumer groups', () => {
  const mkMsg = (offset: string) => ({
    offset,
    value: Buffer.from(offset),
    key: null,
    headers: {},
    timestamp: '',
  });

  /** Registry wired to one mocked consumer; `config()` is what run() got. */
  const makeHarness = (run = jest.fn().mockResolvedValue(undefined)) => {
    const consumer = {
      connect: jest.fn(),
      subscribe: jest.fn(),
      run,
      disconnect: jest.fn(),
    };
    const core = {
      getKafka: jest.fn().mockReturnValue({
        consumer: jest.fn().mockReturnValue(consumer),
        admin: jest.fn().mockReturnValue({
          connect: jest.fn(),
          disconnect: jest.fn(),
          listTopics: jest.fn().mockResolvedValue([]),
          createTopics: jest.fn(),
        }),
      }),
    };
    const registry = new ConsumerRegistryService(
      core as any,
      new BatchProcessorService(),
      { filterDuplicates: (m: any[]) => m, stopCleanup: jest.fn() } as any,
      {} as any, // dlqService — not used in these paths
      undefined,
      undefined,
    );
    const config = () => run.mock.calls[0][0];
    return { registry, config };
  };

  const consumerMeta = (topic: string, options: any, handler = jest.fn()) =>
    ({
      topic,
      connection: 'default',
      options,
      target: { h: handler },
      methodName: 'h',
    }) as any;

  const batchPayload = (
    messages: any[],
    resolveOffset: jest.Mock,
    isStale: () => boolean = () => false,
  ) => ({
    batch: { topic: 't', partition: 0, messages },
    isRunning: () => true,
    isStale,
    resolveOffset,
    heartbeat: () => {},
  });

  it('startAll() rejects when run() rejects', async () => {
    const { registry } = makeHarness(
      jest.fn().mockRejectedValue(new Error('not connected')),
    );
    registry.registerConsumers([consumerMeta('t', {})]);
    await expect(registry.startAll()).rejects.toThrow('not connected');
  });

  it.each([
    [true, false],
    [false, true],
  ])(
    'rejects a group mixing batch=%p and batch=%p consumers',
    (first, second) => {
      const { registry } = makeHarness();
      expect(() =>
        registry.registerConsumers([
          consumerMeta('a', { groupId: 'g', batch: first }),
          consumerMeta('b', { groupId: 'g', batch: second }),
        ]),
      ).toThrow(
        'Consumer group "g" mixes batch and non-batch consumers; use separate groupIds',
      );
    },
  );

  it('resolves offsets only after successful flush (at-least-once)', async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const { registry, config } = makeHarness();
    registry.registerConsumers([
      consumerMeta('t', { batch: true, batchSize: 2 }, handler),
    ]);
    await registry.startAll();

    const resolveOffset = jest.fn();
    await config().eachBatch(
      batchPayload([mkMsg('1'), mkMsg('2')], resolveOffset),
    );

    expect(handler).toHaveBeenCalledTimes(1);
    expect(resolveOffset).toHaveBeenCalledTimes(1);
    expect(resolveOffset).toHaveBeenCalledWith('2');
  });

  it('chunks a fetched batch by batchSize and resolves once', async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const { registry, config } = makeHarness();
    registry.registerConsumers([
      consumerMeta('t', { batch: true, batchSize: 2 }, handler),
    ]);
    await registry.startAll();

    const resolveOffset = jest.fn();
    await config().eachBatch(
      batchPayload(['1', '2', '3', '4', '5'].map(mkMsg), resolveOffset),
    );

    expect(handler.mock.calls.map((c) => c[0].length)).toEqual([2, 2, 1]);
    expect(resolveOffset).toHaveBeenCalledTimes(1);
    expect(resolveOffset).toHaveBeenCalledWith('5');
  });

  it('stops at a stale payload and resolves only what was processed', async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const { registry, config } = makeHarness();
    registry.registerConsumers([
      consumerMeta('t', { batch: true, batchSize: 2 }, handler),
    ]);
    await registry.startAll();

    const resolveOffset = jest.fn();
    const isStale = jest.fn().mockReturnValueOnce(false).mockReturnValue(true);
    await config().eachBatch(
      batchPayload(['1', '2', '3', '4'].map(mkMsg), resolveOffset, isStale),
    );

    expect(handler).toHaveBeenCalledTimes(1);
    expect(resolveOffset).toHaveBeenCalledTimes(1);
    expect(resolveOffset).toHaveBeenCalledWith('2');
  });
});

describe('DLQ on a separate connection', () => {
  const mkAdmin = () => {
    const existing: string[] = [];
    return {
      connect: jest.fn(),
      disconnect: jest.fn(),
      listTopics: jest.fn(() => Promise.resolve([...existing])),
      createTopics: jest.fn(({ topics }: any) => {
        existing.push(topics[0].topic);
        return Promise.resolve();
      }),
    };
  };
  const makeRegistry = (connections: string[]) => {
    const admins: Record<string, ReturnType<typeof mkAdmin>> = {};
    const core = {
      getKafka: jest.fn((name: string) => {
        if (!connections.includes(name)) {
          throw new Error(`Kafka connection "${name}" not found`);
        }
        admins[name] ??= mkAdmin();
        return {
          consumer: jest.fn().mockReturnValue({
            connect: jest.fn(),
            subscribe: jest.fn(),
            run: jest.fn().mockResolvedValue(undefined),
            disconnect: jest.fn(),
          }),
          admin: jest.fn().mockReturnValue(admins[name]),
        };
      }),
    };
    const registry = new ConsumerRegistryService(
      core as any,
      {} as any,
      { stopCleanup: jest.fn() } as any,
      {} as any,
      undefined,
      undefined,
    );
    return { registry, admins };
  };
  const mkConsumer = (dlq: any) =>
    ({
      topic: 'orders',
      connection: 'clusterA',
      options: { allowAutoTopicCreation: true, dlq },
      target: { h: async () => {} },
      methodName: 'h',
    }) as any;
  const created = (admin?: ReturnType<typeof mkAdmin>) =>
    (admin?.createTopics.mock.calls ?? []).map((c) => c[0].topics[0].topic);

  it('fails fast at registration when dlq.connection is unknown', () => {
    const { registry } = makeRegistry(['clusterA']);
    expect(() =>
      registry.registerConsumers([
        mkConsumer({ topic: 'orders-dlq', connection: 'missing' }),
      ]),
    ).toThrow('Kafka connection "missing" not found');
  });

  it('auto-creates the DLQ topic on the DLQ connection', async () => {
    const { registry, admins } = makeRegistry(['clusterA', 'dlqCluster']);
    registry.registerConsumers([
      mkConsumer({ topic: 'orders-dlq', connection: 'dlqCluster' }),
    ]);
    await (registry as any).startAll();
    expect(created(admins.clusterA)).toEqual(['orders']);
    expect(created(admins.dlqCluster)).toEqual(['orders-dlq']);
  });

  it('auto-creates the DLQ topic on the consumer connection by default', async () => {
    const { registry, admins } = makeRegistry(['clusterA']);
    registry.registerConsumers([mkConsumer({ topic: 'orders-dlq' })]);
    await (registry as any).startAll();
    expect(created(admins.clusterA)).toEqual(['orders', 'orders-dlq']);
  });
});
