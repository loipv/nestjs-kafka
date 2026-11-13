import { Logger } from '@nestjs/common';
import { ConsumerRegistryService } from './consumer-registry.service';
import { BatchProcessorService } from './batch-processor.service';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript');

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

  it('keeps a module retry default when the decorator sets that field to undefined', () => {
    const registry = makeRegistry({
      retry: { initialRetryTime: 500, retries: 7 },
    });
    const merged = (registry as any).mergeWithDefaults({
      retry: { initialRetryTime: undefined, retries: 2 },
    });
    expect(merged.retry).toEqual({ initialRetryTime: 500, retries: 2 });
  });

  it('uses module default retry when decorator has none', () => {
    const registry = makeRegistry({ retry: { retries: 7 } });
    const merged = (registry as any).mergeWithDefaults({});
    expect(merged.retry).toEqual({ retries: 7 });
  });

  it('applies each module default only when the decorator leaves it undefined', () => {
    const keys = [
      'partitionAssigners',
      'allowAutoTopicCreation',
      'sessionTimeout',
      'heartbeatInterval',
      'rebalanceTimeout',
      'autoCommit',
      'autoCommitInterval',
      'fromBeginning',
      'autoCreateTopicPartitions',
      'autoCreateTopicReplicationFactor',
    ];
    for (const key of keys) {
      const registry = makeRegistry({ [key]: 'D' });
      const merge = (options: any) =>
        (registry as any).mergeWithDefaults(options)[key];
      expect([key, merge({})]).toEqual([key, 'D']);
      expect([key, merge({ [key]: undefined })]).toEqual([key, 'D']);
      expect([key, merge({ [key]: 'X' })]).toEqual([key, 'X']);
    }
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

  it('evaluateRetry caps the delay at retry.maxRetryTime', () => {
    const r = makeRegistry();
    expect(
      (r as any).evaluateRetry(10, {
        retry: { initialRetryTime: 1000, maxRetryTime: 5000 },
      }),
    ).toEqual({ action: 'retry', delayMs: 5000 });
  });

  it('no longer retries in-process', () => {
    const r = makeRegistry() as any;
    expect(r.runWithRetry).toBeUndefined();
    expect(r.sleep).toBeUndefined();
  });
});

describe('timer option bounds', () => {
  describe.each(['retry.multiplier', 'dlq.retryBackoffMultiplier'])(
    '%s',
    (path) => {
      it.each([1, 1.5, 3])('accepts %p', (value) => {
        expect(() => register(withOption(path, value))).not.toThrow();
      });

      it.each([0.5, 0, -2, NaN, Infinity])('rejects %p', (value) => {
        expect(() => register(withOption(path, value))).toThrow(
          `Consumer "t": ${path} must be a finite number >= 1`,
        );
      });
    },
  );

  const makeRegistry = () =>
    new ConsumerRegistryService(
      { getKafka: jest.fn() } as any,
      {} as any,
      {} as any,
      {} as any,
      undefined,
      undefined,
    );
  const register = (options: any) =>
    makeRegistry().registerConsumers([
      {
        topic: 't',
        connection: 'default',
        options,
        target: { h: jest.fn() },
        methodName: 'h',
      } as any,
    ]);
  const withOption = (path: string, value: number) => {
    const [group, key] = path.split('.');
    return group === 'retry'
      ? { retry: { [key]: value } }
      : { dlq: { topic: 'd', [key]: value } };
  };

  describe.each([
    'retry.maxRetryTime',
    'retry.initialRetryTime',
    'dlq.sendTimeout',
    'dlq.retryDelay',
  ])('%s', (path) => {
    it('accepts 2147483647', () => {
      expect(() => register(withOption(path, 2147483647))).not.toThrow();
    });

    it('rejects 2147483648', () => {
      expect(() => register(withOption(path, 2147483648))).toThrow(
        `Consumer "t": ${path} must be at most 2147483647 ms`,
      );
    });

    it.each([0, -1, NaN])('rejects %p', (value) => {
      expect(() => register(withOption(path, value))).toThrow(
        `Consumer "t": ${path} must be a positive number`,
      );
    });
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

  /**
   * Registry wired to one mocked consumer; `config()` is what run() got,
   * `consumerConfig()` what the consumer was constructed with.
   */
  const makeHarness = (
    run = jest.fn().mockResolvedValue(undefined),
    deps: { dlqService?: any; idempotency?: any } = {},
  ) => {
    const consumer = {
      connect: jest.fn(),
      subscribe: jest.fn(),
      run,
      disconnect: jest.fn(),
      resume: jest.fn(),
      commitOffsets: jest.fn().mockResolvedValue(undefined),
    };
    const consumerFactory = jest.fn().mockReturnValue(consumer);
    const core = {
      setConsumerStatus: jest.fn(),
      getKafka: jest.fn().mockReturnValue({
        consumer: consumerFactory,
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
      deps.idempotency ??
        ({
          filterDuplicates: (m: any[]) => m,
          isProcessed: () => false,
          markProcessed: jest.fn(),
          stopCleanup: jest.fn(),
        } as any),
      deps.dlqService ?? ({} as any),
      undefined,
      undefined,
    );
    const config = () => run.mock.calls[0][0];
    const consumerConfig = () => consumerFactory.mock.calls[0][0];
    return { registry, config, consumer, consumerConfig, core };
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
    pause: jest.Mock = jest.fn(() => jest.fn()),
  ) => ({
    batch: { topic: 't', partition: 0, messages },
    isRunning: () => true,
    isStale,
    resolveOffset,
    pause,
    heartbeat: () => {},
    commitOffsetsIfNecessary: jest.fn().mockResolvedValue(undefined),
  });

  const messagePayload = (
    offset = '5',
    pause: jest.Mock = jest.fn(() => jest.fn()),
  ) => ({
    topic: 't',
    partition: 0,
    message: mkMsg(offset),
    pause,
    heartbeat: () => {},
  });

  /** Registers one consumer for topic 't' and starts it. */
  const started = async (
    options: any,
    handler: jest.Mock,
    deps: { dlqService?: any; idempotency?: any } = {},
  ) => {
    const h = makeHarness(undefined, deps);
    h.registry.registerConsumers([consumerMeta('t', options, handler)]);
    await h.registry.startAll();
    return h;
  };

  describe('autoCommit: false', () => {
    const commitOf = (offset: string, topic = 't') => [
      [{ topic, partition: 0, offset }],
    ];

    it('eachMessage commits offset + 1 after success', async () => {
      const { config, consumer } = await started(
        { autoCommit: false },
        jest.fn(),
      );
      await config().eachMessage(messagePayload('5'));
      expect(consumer.commitOffsets.mock.calls).toEqual([commitOf('6')]);
    });

    it('eachMessage commits exactly above 2^53', async () => {
      const { config, consumer } = await started(
        { autoCommit: false },
        jest.fn(),
      );
      await config().eachMessage(messagePayload('9007199254740993'));
      expect(consumer.commitOffsets.mock.calls).toEqual([
        commitOf('9007199254740994'),
      ]);
    });

    it('eachMessage commits after a complete verdict (skip)', async () => {
      jest.useFakeTimers();
      try {
        const { config, consumer } = await started(
          {
            autoCommit: false,
            retry: { retries: 1, skipMessageOnMaxRetries: true },
          },
          jest.fn().mockRejectedValue(new Error('x')),
        );
        await expect(config().eachMessage(messagePayload())).rejects.toThrow(
          'x',
        );
        expect(consumer.commitOffsets).not.toHaveBeenCalled();
        await config().eachMessage(messagePayload());
        expect(consumer.commitOffsets.mock.calls).toEqual([commitOf('6')]);
      } finally {
        jest.useRealTimers();
      }
    });

    it('eachMessage commits after a DLQ complete verdict', async () => {
      const dlqService = {
        handleFailure: jest.fn().mockResolvedValue({ action: 'complete' }),
      };
      const { config, consumer } = await started(
        { autoCommit: false, dlq: { topic: 'd' } },
        jest.fn().mockRejectedValue(new Error('x')),
        { dlqService },
      );
      await config().eachMessage(messagePayload('5'));
      expect(dlqService.handleFailure).toHaveBeenCalledTimes(1);
      expect(consumer.commitOffsets.mock.calls).toEqual([commitOf('6')]);
    });

    it('eachMessage commits a duplicate skipped by idempotency', async () => {
      const handler = jest.fn();
      const { config, consumer } = await started(
        { autoCommit: false, idempotencyKey: () => 'k' },
        handler,
        {
          idempotency: {
            isProcessed: () => true,
            markProcessed: jest.fn(),
            stopCleanup: jest.fn(),
          },
        },
      );
      await config().eachMessage(messagePayload('5'));
      expect(handler).not.toHaveBeenCalled();
      expect(consumer.commitOffsets.mock.calls).toEqual([commitOf('6')]);
    });

    it('eachMessage commits a message for a topic without a handler', async () => {
      const { config, consumer } = await started(
        { autoCommit: false },
        jest.fn(),
      );
      await config().eachMessage({ ...messagePayload('5'), topic: 'other' });
      expect(consumer.commitOffsets.mock.calls).toEqual([
        commitOf('6', 'other'),
      ]);
    });

    it('eachMessage does not commit when the handler fails and is retried', async () => {
      jest.useFakeTimers();
      try {
        const { config, consumer } = await started(
          { autoCommit: false },
          jest.fn().mockRejectedValue(new Error('x')),
        );
        await expect(config().eachMessage(messagePayload())).rejects.toThrow(
          'x',
        );
        expect(consumer.commitOffsets).not.toHaveBeenCalled();
      } finally {
        jest.useRealTimers();
      }
    });

    it('eachMessage does not commit under auto-commit', async () => {
      const { config, consumer } = await started({}, jest.fn());
      await config().eachMessage(messagePayload('5'));
      expect(consumer.commitOffsets).not.toHaveBeenCalled();
    });

    it('eachMessage logs and does not throw when the commit fails', async () => {
      const warn = jest
        .spyOn(Logger.prototype, 'warn')
        .mockImplementation(() => undefined);
      try {
        const { config, consumer } = await started(
          { autoCommit: false },
          jest.fn(),
        );
        consumer.commitOffsets.mockRejectedValue(new Error('ERR__STATE'));
        await expect(
          config().eachMessage(messagePayload('5')),
        ).resolves.toBeUndefined();
        expect(warn).toHaveBeenCalledWith(
          'Commit failed for t[0]@6: Error: ERR__STATE',
        );
      } finally {
        warn.mockRestore();
      }
    });

    it('eachBatch commits after every resolved chunk', async () => {
      const { config } = await started(
        { batch: true, batchSize: 2, autoCommit: false },
        jest.fn(),
      );
      const resolveOffset = jest.fn();
      const payload = batchPayload(
        ['1', '2', '3', '4'].map(mkMsg),
        resolveOffset,
      );
      await config().eachBatch(payload);
      expect(resolveOffset.mock.calls).toEqual([['2'], ['4']]);
      const commit = payload.commitOffsetsIfNecessary;
      expect(commit).toHaveBeenCalledTimes(2);
      expect(commit.mock.invocationCallOrder[0]).toBeGreaterThan(
        resolveOffset.mock.invocationCallOrder[0],
      );
      expect(commit.mock.invocationCallOrder[0]).toBeLessThan(
        resolveOffset.mock.invocationCallOrder[1],
      );
    });

    it('eachBatch treats ERR__NO_OFFSET (nothing new to commit) as success', async () => {
      const warn = jest
        .spyOn(Logger.prototype, 'warn')
        .mockImplementation(() => undefined);
      try {
        const { config } = await started(
          { batch: true, batchSize: 2, autoCommit: false },
          jest.fn(),
        );
        const payload = batchPayload(['1', '2'].map(mkMsg), jest.fn());
        payload.commitOffsetsIfNecessary.mockRejectedValue(
          Object.assign(new Error('Local: No offset stored'), { code: -168 }),
        );
        await expect(config().eachBatch(payload)).resolves.toBeUndefined();
        expect(warn).not.toHaveBeenCalled();
      } finally {
        warn.mockRestore();
      }
    });

    it('eachBatch logs and does not throw when the commit fails', async () => {
      const warn = jest
        .spyOn(Logger.prototype, 'warn')
        .mockImplementation(() => undefined);
      try {
        const { config } = await started(
          { batch: true, batchSize: 2, autoCommit: false },
          jest.fn(),
        );
        const payload = batchPayload(['1', '2'].map(mkMsg), jest.fn());
        payload.commitOffsetsIfNecessary.mockRejectedValue(
          new Error('ERR__STATE'),
        );
        await expect(config().eachBatch(payload)).resolves.toBeUndefined();
        expect(warn).toHaveBeenCalledWith(
          'Commit failed for t[0]@3: Error: ERR__STATE',
        );
      } finally {
        warn.mockRestore();
      }
    });

    it('eachBatch does not commit under auto-commit', async () => {
      const { config } = await started(
        { batch: true, batchSize: 2 },
        jest.fn(),
      );
      const payload = batchPayload(['1', '2'].map(mkMsg), jest.fn());
      await config().eachBatch(payload);
      expect(payload.commitOffsetsIfNecessary).not.toHaveBeenCalled();
    });
  });

  describe('retry by pause and rethrow', () => {
    beforeEach(() => jest.useFakeTimers());
    afterEach(() => jest.useRealTimers());

    it('eachMessage: a failure pauses the partition, throws, and resumes after the delay', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('x'));
      const { config } = await started(
        { retry: { initialRetryTime: 100 } },
        handler,
      );
      const resumeFn = jest.fn();
      const pause = jest.fn(() => resumeFn);

      await expect(
        config().eachMessage(messagePayload('5', pause)),
      ).rejects.toThrow('x');
      expect(pause).toHaveBeenCalledTimes(1);
      jest.advanceTimersByTime(99);
      expect(resumeFn).not.toHaveBeenCalled();
      jest.advanceTimersByTime(1);
      expect(resumeFn).toHaveBeenCalledTimes(1);
    });

    it('eachMessage: attempts grow across redeliveries and reach the skip', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('x'));
      const { config } = await started(
        { retry: { retries: 2, skipMessageOnMaxRetries: true } },
        handler,
      );
      await expect(config().eachMessage(messagePayload())).rejects.toThrow('x');
      await expect(config().eachMessage(messagePayload())).rejects.toThrow('x');
      await expect(
        config().eachMessage(messagePayload()),
      ).resolves.toBeUndefined();
    });

    it('eachMessage: success after failures clears the counter', async () => {
      const handler = jest
        .fn()
        .mockRejectedValueOnce(new Error('1'))
        .mockRejectedValueOnce(new Error('2'))
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(new Error('4'));
      const { config, registry } = await started(
        { retry: { initialRetryTime: 100, multiplier: 2 } },
        handler,
      );
      const schedule = jest.spyOn(
        (registry as any).retryTracker,
        'scheduleResume',
      );
      for (let i = 0; i < 4; i++) {
        await config()
          .eachMessage(messagePayload())
          .catch(() => {});
      }
      expect(schedule.mock.calls.map((c) => c[1])).toEqual([100, 200, 100]);
    });

    it('eachMessage: DLQ complete acks without pausing', async () => {
      const dlqService = {
        handleFailure: jest.fn().mockResolvedValue({ action: 'complete' }),
      };
      const pause = jest.fn(() => jest.fn());
      const { config } = await started(
        { dlq: { topic: 'd' } },
        jest.fn().mockRejectedValue(new Error('x')),
        { dlqService },
      );
      await expect(
        config().eachMessage(messagePayload('5', pause)),
      ).resolves.toBeUndefined();
      expect(pause).not.toHaveBeenCalled();
      expect(dlqService.handleFailure.mock.calls[0][0]).toEqual([
        expect.objectContaining({ offset: '5' }),
      ]);
    });

    it('eachMessage: passes retry.maxRetryTime to the DLQ verdict as the cap', async () => {
      const dlqService = {
        handleFailure: jest
          .fn()
          .mockResolvedValue({ action: 'retry', delayMs: 10 }),
      };
      const { config } = await started(
        { dlq: { topic: 'd' }, retry: { maxRetryTime: 4000 } },
        jest.fn().mockRejectedValue(new Error('x')),
        { dlqService },
      );
      await expect(config().eachMessage(messagePayload())).rejects.toThrow('x');
      expect(dlqService.handleFailure.mock.calls[0][7]).toBe(4000);
    });

    it('eachMessage: throws the handler error when pause fails during disconnect', async () => {
      const pause = jest.fn(() => {
        throw new Error('Pause can only be called while connected.');
      });
      const { config } = await started(
        {},
        jest.fn().mockRejectedValue(new Error('x')),
      );
      await expect(
        config().eachMessage(messagePayload('5', pause)),
      ).rejects.toThrow('x');
    });

    it('eachMessage: throws while shutting down instead of acking', async () => {
      const handler = jest.fn();
      const { config, registry } = await started({}, handler);
      await registry.gracefulShutdown();
      await expect(config().eachMessage(messagePayload())).rejects.toThrow(
        'Consumer is shutting down',
      );
      expect(handler).not.toHaveBeenCalled();
    });

    it('eachBatch: resolves each successful chunk and pauses on the failing one', async () => {
      const handler = jest
        .fn()
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(new Error('x'));
      const { config } = await started({ batch: true, batchSize: 2 }, handler);
      const resolveOffset = jest.fn();
      const pause = jest.fn(() => jest.fn());

      await expect(
        config().eachBatch(
          batchPayload(
            ['1', '2', '3', '4'].map(mkMsg),
            resolveOffset,
            undefined,
            pause,
          ),
        ),
      ).rejects.toThrow('x');
      expect(resolveOffset.mock.calls).toEqual([['2']]);
      expect(pause).toHaveBeenCalledTimes(1);
    });

    it('eachBatch: an all-duplicate chunk is resolved without calling the handler', async () => {
      const handler = jest.fn().mockRejectedValue(new Error('x'));
      const idempotency = {
        filterDuplicates: jest
          .fn()
          .mockReturnValueOnce([])
          .mockImplementation((m: any[]) => m),
        markProcessed: jest.fn(),
        stopCleanup: jest.fn(),
      };
      const { config } = await started(
        { batch: true, batchSize: 2, idempotencyKey: () => 'k' },
        handler,
        { idempotency },
      );
      const resolveOffset = jest.fn();
      const pause = jest.fn(() => jest.fn());

      await expect(
        config().eachBatch(
          batchPayload(
            ['1', '2', '3', '4'].map(mkMsg),
            resolveOffset,
            undefined,
            pause,
          ),
        ),
      ).rejects.toThrow('x');
      expect(handler).toHaveBeenCalledTimes(1);
      expect(resolveOffset.mock.calls).toEqual([['2']]);
      // Only the failing second chunk pauses, never the all-duplicate first one
      expect(pause).toHaveBeenCalledTimes(1);
    });

    it('eachBatch: DLQ complete dead-letters only the handled messages and resolves the chunk', async () => {
      const dlqService = {
        handleFailure: jest.fn().mockResolvedValue({ action: 'complete' }),
      };
      const idempotency = {
        filterDuplicates: jest.fn((m: any[]) =>
          m.filter((x) => x.offset !== '1'),
        ),
        markProcessed: jest.fn(),
        stopCleanup: jest.fn(),
      };
      const { config } = await started(
        {
          batch: true,
          batchSize: 2,
          idempotencyKey: () => 'k',
          dlq: { topic: 'd' },
        },
        jest.fn().mockRejectedValue(new Error('x')),
        { dlqService, idempotency },
      );
      const resolveOffset = jest.fn();

      await expect(
        config().eachBatch(batchPayload(['1', '2'].map(mkMsg), resolveOffset)),
      ).resolves.toBeUndefined();
      expect(
        dlqService.handleFailure.mock.calls[0][0].map((m: any) => m.offset),
      ).toEqual(['2']);
      expect(resolveOffset).toHaveBeenCalledWith('2');
    });

    it('eachMessage: a handler rejecting with undefined still throws an Error (seek-back stays safe)', async () => {
      const pause = jest.fn(() => jest.fn());
      const { config } = await started(
        {},
        jest.fn().mockRejectedValue(undefined),
      );
      const err = await config()
        .eachMessage(messagePayload('5', pause))
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(Error);
      expect((err as Error).message).toContain('non-Error');
      expect(pause).toHaveBeenCalledTimes(1);
    });

    it('eachBatch: a handler rejecting with null still throws an Error', async () => {
      const { config } = await started(
        { batch: true, batchSize: 2 },
        jest.fn().mockRejectedValue(null),
      );
      const err = await config()
        .eachBatch(batchPayload(['1', '2'].map(mkMsg), jest.fn()))
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(Error);
    });

    it('eachMessage: a throwing idempotencyKey extractor goes through the retry path', async () => {
      const idempotency = {
        isProcessed: () => {
          throw new Error('no id header');
        },
        markProcessed: jest.fn(),
        stopCleanup: jest.fn(),
      };
      const pause = jest.fn(() => jest.fn());
      const { config } = await started(
        { idempotencyKey: () => 'k' },
        jest.fn(),
        { idempotency },
      );
      await expect(
        config().eachMessage(messagePayload('5', pause)),
      ).rejects.toThrow('no id header');
      expect(pause).toHaveBeenCalledTimes(1);
    });

    it('eachBatch: a throwing idempotencyKey extractor goes through the retry path', async () => {
      const idempotency = {
        filterDuplicates: () => {
          throw new Error('no id header');
        },
        markProcessed: jest.fn(),
        stopCleanup: jest.fn(),
      };
      const pause = jest.fn(() => jest.fn());
      const { config } = await started(
        { batch: true, idempotencyKey: () => 'k' },
        jest.fn(),
        { idempotency },
      );
      await expect(
        config().eachBatch(
          batchPayload(['1'].map(mkMsg), jest.fn(), undefined, pause),
        ),
      ).rejects.toThrow('no id header');
      expect(pause).toHaveBeenCalledTimes(1);
    });

    it('eachBatch: stops between chunks once shutdown starts', async () => {
      const ref: { registry?: any } = {};
      const handler = jest.fn(async () => {
        await ref.registry.gracefulShutdown();
      });
      const { config, registry } = await started(
        { batch: true, batchSize: 2 },
        handler,
      );
      ref.registry = registry;
      const resolveOffset = jest.fn();
      await config().eachBatch(
        batchPayload(['1', '2', '3', '4'].map(mkMsg), resolveOffset),
      );
      expect(handler).toHaveBeenCalledTimes(1);
      expect(resolveOffset.mock.calls).toEqual([['2']]);
    });

    it('gracefulShutdown cancels pending resume timers', async () => {
      const { config, registry } = await started(
        { retry: { initialRetryTime: 60_000 } },
        jest.fn().mockRejectedValue(new Error('x')),
      );
      await expect(config().eachMessage(messagePayload())).rejects.toThrow('x');
      expect(jest.getTimerCount()).toBe(1);
      await registry.gracefulShutdown();
      expect(jest.getTimerCount()).toBe(0);
    });
  });

  it('rejects a second handler for the same topic in one group', () => {
    const { registry } = makeHarness();
    expect(() =>
      registry.registerConsumers([
        consumerMeta('t', { groupId: 'g' }),
        consumerMeta('t', { groupId: 'g' }),
      ]),
    ).toThrow('Consumer group "g" already has a handler for topic "t"');
  });

  it('creates the consumer at start with the largest batchSize', async () => {
    const { registry, consumerConfig } = makeHarness();
    registry.registerConsumers([
      consumerMeta('a', { groupId: 'g', batch: true, batchSize: 250 }),
      consumerMeta('b', { groupId: 'g', batch: true, batchSize: 10 }),
    ]);
    await registry.startAll();
    expect(consumerConfig()['js.consumer.max.batch.size']).toBe(250);
  });

  it.each([0, 1.5, undefined])(
    'falls back to 100 for batchSize %p',
    async (batchSize) => {
      const { registry, consumerConfig } = makeHarness();
      registry.registerConsumers([
        consumerMeta('t', { batch: true, batchSize }),
      ]);
      await registry.startAll();
      expect(consumerConfig()['js.consumer.max.batch.size']).toBe(100);
    },
  );

  it('does not forward handler retry options to the client backoff', async () => {
    const { registry, consumerConfig } = makeHarness();
    registry.registerConsumers([
      consumerMeta('t', {
        retry: { maxRetryTime: 600000, initialRetryTime: 1 },
      }),
    ]);
    await registry.startAll();
    expect(consumerConfig().kafkaJS.retry).toBeUndefined();
  });

  it.each([0, -1, Number.NaN])(
    'rejects dlq.sendTimeout %p at registration',
    (sendTimeout) => {
      const { registry } = makeHarness();
      expect(() =>
        registry.registerConsumers([
          consumerMeta('t', { dlq: { topic: 'd', sendTimeout } }),
        ]),
      ).toThrow('Consumer "t": dlq.sendTimeout must be a positive number');
    },
  );

  it('sets no max batch size for a non-batch group', async () => {
    const { registry, consumerConfig } = makeHarness();
    registry.registerConsumers([consumerMeta('t', {})]);
    await registry.startAll();
    expect(consumerConfig()).not.toHaveProperty('js.consumer.max.batch.size');
  });

  it('disconnects the consumer when subscribe fails after connect', async () => {
    const { registry, consumer } = makeHarness();
    consumer.subscribe.mockRejectedValue(new Error('boom'));
    registry.registerConsumers([consumerMeta('t', {})]);
    await expect(registry.startAll()).rejects.toThrow('boom');
    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
  });

  it('on revoke, resumes the revoked partitions and clears their retry state', async () => {
    jest.useFakeTimers();
    try {
      const { registry, consumer, consumerConfig } = makeHarness();
      registry.registerConsumers([consumerMeta('t', {})]);
      await registry.startAll();
      const tracker = (registry as any).retryTracker;
      const resumeFn = jest.fn();
      tracker.nextAttempt('default:t-group|t|0|5');
      tracker.scheduleResume('default:t-group|t|0', 100, resumeFn);

      consumerConfig().rebalance_cb({ code: -174 }, [
        { topic: 't', partition: 0 },
      ]);

      expect(consumer.resume).toHaveBeenCalledWith([
        { topic: 't', partitions: [0] },
      ]);
      jest.advanceTimersByTime(100);
      expect(resumeFn).not.toHaveBeenCalled();
      expect(tracker.nextAttempt('default:t-group|t|0|5')).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });

  it('ignores assign events', async () => {
    const { registry, consumer, consumerConfig } = makeHarness();
    registry.registerConsumers([consumerMeta('t', {})]);
    await registry.startAll();
    consumerConfig().rebalance_cb({ code: -175 }, [
      { topic: 't', partition: 0 },
    ]);
    expect(consumer.resume).not.toHaveBeenCalled();
  });

  it('gracefulShutdown runs once when called twice', async () => {
    const { registry, consumer } = makeHarness();
    registry.registerConsumers([consumerMeta('t', {})]);
    await registry.startAll();
    await Promise.all([
      registry.gracefulShutdown(),
      registry.gracefulShutdown(),
    ]);
    await registry.gracefulShutdown();
    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
  });

  it('onModuleDestroy runs the memoised graceful shutdown', async () => {
    const { registry, consumer } = makeHarness();
    registry.registerConsumers([consumerMeta('t', {})]);
    await registry.startAll();
    await (registry as any).onModuleDestroy();
    await registry.gracefulShutdown();
    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
    expect((registry as any).onApplicationShutdown).toBeUndefined();
  });

  it('reports starting, running and stopped', async () => {
    const { registry, core } = makeHarness();
    registry.registerConsumers([consumerMeta('t', {})]);
    await registry.startAll();
    await registry.gracefulShutdown();
    expect(core.setConsumerStatus.mock.calls).toEqual([
      ['default:t-group', 'starting'],
      ['default:t-group', 'running'],
      ['default:t-group', 'stopped'],
    ]);
  });

  it('reports failed when start throws', async () => {
    const { registry, core } = makeHarness(
      jest.fn().mockRejectedValue(new Error('nope')),
    );
    registry.registerConsumers([consumerMeta('t', {})]);
    await expect(registry.startAll()).rejects.toThrow('nope');
    expect(core.setConsumerStatus.mock.calls.at(-1)).toEqual([
      'default:t-group',
      'failed',
    ]);
  });

  it('startAll() waits for every group before rejecting', async () => {
    let finishB!: () => void;
    const consumerA = {
      connect: jest.fn().mockRejectedValue(new Error('a down')),
      subscribe: jest.fn(),
      run: jest.fn(),
      disconnect: jest.fn(),
    };
    const consumerB = {
      connect: jest.fn(
        () =>
          new Promise<void>((r) => {
            finishB = r;
          }),
      ),
      subscribe: jest.fn(),
      run: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn(),
    };
    const factory = jest
      .fn()
      .mockReturnValueOnce(consumerA)
      .mockReturnValueOnce(consumerB);
    const registry = new ConsumerRegistryService(
      {
        setConsumerStatus: jest.fn(),
        getKafka: () => ({ consumer: factory }),
      } as any,
      new BatchProcessorService(),
      { stopCleanup: jest.fn() } as any,
      {} as any,
      undefined,
      undefined,
    );
    registry.registerConsumers([consumerMeta('a', {}), consumerMeta('b', {})]);

    let settled = false;
    const starting = registry.startAll().catch((e: Error) => {
      settled = true;
      return e;
    });
    await new Promise((r) => setImmediate(r));
    expect(settled).toBe(false); // group b is still starting
    finishB();
    expect(((await starting) as Error).message).toBe('a down');
  });

  it('abortStartup disconnects started groups and every producer', async () => {
    const { registry, consumer, core } = makeHarness();
    (core as any).disconnectAll = jest.fn().mockResolvedValue(undefined);
    registry.registerConsumers([consumerMeta('t', {})]);
    await registry.startAll();

    await registry.abortStartup();

    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
    expect((core as any).disconnectAll).toHaveBeenCalledTimes(1);
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

  it('chunks a fetched batch by batchSize and resolves each chunk', async () => {
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
    expect(resolveOffset.mock.calls).toEqual([['2'], ['4'], ['5']]);
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
      setConsumerStatus: jest.fn(),
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
