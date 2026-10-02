import { ConsumerRegistryService } from './consumer-registry.service';
import { BatchProcessorService } from './batch-processor.service';

describe('ConsumerRegistryService.mergeWithDefaults', () => {
  const makeRegistry = (moduleOptions?: any) =>
    new ConsumerRegistryService(
      {} as any,
      {} as any,
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
      { register: jest.fn(), setTopics: jest.fn() } as any,
      { handleFailure: jest.fn() } as any,
      {
        registerOriginalGroupId: jest.fn(),
        gracefulShutdown: jest.fn().mockResolvedValue(undefined),
      } as any,
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

  it('DLQ send failure propagates (infra crash path → auto-restart)', async () => {
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

describe('consumer auto-restart', () => {
  const makeRegistry = () =>
    new ConsumerRegistryService(
      {} as any,
      {} as any,
      { stopCleanup: jest.fn() } as any,
      { register: jest.fn(), setTopics: jest.fn() } as any,
      { handleFailure: jest.fn() } as any,
      {
        registerOriginalGroupId: jest.fn(),
        startAll: jest.fn().mockResolvedValue(undefined),
        gracefulShutdown: jest.fn().mockResolvedValue(undefined),
      } as any,
      undefined,
      undefined,
    );

  it('computeRestartDelay grows exponentially and caps at maxRetryTime', () => {
    const r = makeRegistry();
    const group = {
      options: {
        retry: { initialRetryTime: 1000, multiplier: 2, maxRetryTime: 5000 },
      },
      restartAttempts: 0,
    } as any;
    expect((r as any).computeRestartDelay(group)).toBe(1000);
    group.restartAttempts = 2;
    expect((r as any).computeRestartDelay(group)).toBe(4000);
    group.restartAttempts = 10;
    expect((r as any).computeRestartDelay(group)).toBe(5000); // capped
  });

  it('run() rejection (connection crash) schedules a consumer restart', async () => {
    const restart = jest.fn().mockResolvedValue(undefined);
    const consumer = {
      connect: jest.fn(),
      subscribe: jest.fn(),
      run: jest.fn().mockRejectedValue(new Error('connection reset')),
      disconnect: jest.fn(),
      pause: jest.fn(),
      resume: jest.fn(),
    };
    const core = {
      getKafka: jest.fn().mockReturnValue({
        consumer: jest.fn().mockReturnValue(consumer),
        admin: jest.fn().mockReturnValue({
          connect: jest.fn(),
          disconnect: jest.fn(),
          listTopics: jest.fn().mockResolvedValue([]),
        }),
      }),
    };
    const r = makeRegistry();
    (r as any).kafkaCore = core;
    (r as any).scheduleConsumerRestart = restart;
    r.registerConsumers([
      {
        topic: 't',
        connection: 'default',
        options: {},
        target: { h: async () => {} },
        methodName: 'h',
      } as any,
    ]);
    await (r as any).startAll();
    await new Promise((resolve) => setImmediate(resolve)); // let the .catch microtask run
    expect(restart).toHaveBeenCalledWith(
      expect.objectContaining({ groupId: 't-group' }),
      expect.any(Error),
    );
  });

  it('does not restart while shutting down', async () => {
    const r = makeRegistry();
    const group = {
      groupId: 'g',
      isRestarting: false,
      restartAttempts: 0,
      consumer: { disconnect: jest.fn() },
    } as any;
    await (r as any).gracefulShutdown();
    await (r as any).scheduleConsumerRestart(group, new Error('x'));
    expect(group.restartAttempts).toBe(0); // returned before any sleep/reconnect
  });
});

describe('batch offset semantics', () => {
  it('resolves offsets only after successful flush (at-least-once)', async () => {
    let captured: any;
    const consumer = {
      connect: jest.fn(),
      subscribe: jest.fn(),
      run: jest.fn().mockImplementation((cfg) => {
        captured = cfg;
        return Promise.resolve(undefined);
      }),
      disconnect: jest.fn(),
      pause: jest.fn(),
      resume: jest.fn(),
    };
    const admin = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      listTopics: jest.fn().mockResolvedValue([]),
      createTopics: jest.fn(),
    };
    const core = {
      getKafka: jest.fn().mockReturnValue({
        consumer: jest.fn().mockReturnValue(consumer),
        admin: jest.fn().mockReturnValue(admin),
      }),
    };
    const handler = jest.fn().mockResolvedValue(undefined);
    const registry = new ConsumerRegistryService(
      core as any,
      new BatchProcessorService(),
      { filterDuplicates: (m: any[]) => m } as any,
      { register: jest.fn(), setTopics: jest.fn() } as any,
      {} as any, // dlqService — not used in this path
      {} as any, // dlqRetryService — replaced below
      undefined,
      undefined,
    );
    (registry as any).dlqRetryService = {
      registerOriginalGroupId: jest.fn(),
      startAll: jest.fn().mockResolvedValue(undefined),
      gracefulShutdown: jest.fn().mockResolvedValue(undefined),
    };
    registry.registerConsumers([
      {
        topic: 't',
        connection: 'default',
        options: { batch: true, batchSize: 2, batchTimeout: 5000 },
        target: { h: handler },
        methodName: 'h',
      } as any,
    ]);
    await (registry as any).startAll();

    const msgs = [
      {
        offset: '1',
        value: Buffer.from('a'),
        key: null,
        headers: {},
        timestamp: '',
      },
      {
        offset: '2',
        value: Buffer.from('b'),
        key: null,
        headers: {},
        timestamp: '',
      },
    ];
    const resolveOffset = jest.fn();
    await captured.eachBatch({
      batch: { topic: 't', partition: 0, messages: msgs },
      isRunning: () => true,
      isStale: () => false,
      resolveOffset,
      heartbeat: () => {},
    });

    expect(handler).toHaveBeenCalledTimes(1); // flushed as one batch
    expect(resolveOffset).toHaveBeenCalledTimes(1); // once, not per-add
    expect(resolveOffset).toHaveBeenCalledWith('2'); // last offset, AFTER flush
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
      { register: jest.fn(), setTopics: jest.fn() } as any,
      {} as any,
      {
        registerOriginalGroupId: jest.fn(),
        registerDlqRetryConsumer: jest.fn(),
        startAll: jest.fn().mockResolvedValue(undefined),
      } as any,
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

  it('auto-creates DLQ + final DLQ topics on the DLQ connection', async () => {
    const { registry, admins } = makeRegistry(['clusterA', 'dlqCluster']);
    registry.registerConsumers([
      mkConsumer({
        topic: 'orders-dlq',
        connection: 'dlqCluster',
        retry: { enabled: true, finalDlqTopic: 'orders-final' },
      }),
    ]);
    await (registry as any).startAll();
    expect(created(admins.clusterA)).toEqual(['orders']);
    expect(created(admins.dlqCluster)).toEqual(['orders-dlq', 'orders-final']);
  });

  it('auto-creates the DLQ topic on the consumer connection by default', async () => {
    const { registry, admins } = makeRegistry(['clusterA']);
    registry.registerConsumers([mkConsumer({ topic: 'orders-dlq' })]);
    await (registry as any).startAll();
    expect(created(admins.clusterA)).toEqual(['orders', 'orders-dlq']);
  });
});
