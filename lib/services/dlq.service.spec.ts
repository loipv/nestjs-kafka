import { DlqService } from './dlq.service';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript', () => ({
  KafkaJS: {
    Kafka: jest.fn(),
    logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 },
  },
}));

const mkMsg = (over: Record<string, unknown> = {}) =>
  ({
    offset: '5',
    value: Buffer.from('{}'),
    key: null,
    headers: {},
    timestamp: '',
    ...over,
  }) as any;

function makeService() {
  const kafkaClient = { sendBatch: jest.fn().mockResolvedValue(undefined) };
  const svc = new DlqService(kafkaClient as any);
  return { svc, kafkaClient };
}

describe('DlqService.handleFailure (verdict API)', () => {
  it('returns retry verdict with capped exponential delay', async () => {
    const { svc } = makeService();
    const v = await svc.handleFailure(
      [mkMsg()],
      new Error('x'),
      { topic: 'dlq' },
      't',
      0,
      'default',
      1,
    );
    expect(v).toEqual({ action: 'retry', delayMs: 1000 });
  });

  it('caps delay at MAX_RETRY_DELAY_MS by default', async () => {
    const { svc } = makeService();
    const v = await svc.handleFailure(
      [mkMsg()],
      new Error('x'),
      { topic: 'dlq', retryDelay: 60000, retryBackoffMultiplier: 3 },
      't',
      0,
      'default',
      2,
    );
    expect(v).toEqual({ action: 'retry', delayMs: 30000 });
  });

  it('returns retry with the capped delay while attempt <= maxRetries', async () => {
    const { svc } = makeService();
    const v = await svc.handleFailure(
      [mkMsg()],
      new Error('x'),
      { topic: 'd', retryDelay: 60000 },
      't',
      0,
      undefined,
      1,
      5000,
    );
    expect(v).toEqual({ action: 'retry', delayMs: 5000 });
  });

  it('sends to DLQ and completes when retries exhausted', async () => {
    const { svc, kafkaClient } = makeService();
    const v = await svc.handleFailure(
      [mkMsg()],
      new Error('boom'),
      { topic: 'orders-dlq', maxRetries: 3 },
      'orders',
      0,
      'default',
      4,
    );
    expect(v).toEqual({ action: 'complete' });
    expect(kafkaClient.sendBatch).toHaveBeenCalledWith(
      'orders-dlq',
      [
        expect.objectContaining({
          headers: expect.objectContaining({
            'x-dlq-original-topic': 'orders',
          }),
        }),
      ],
      { connection: 'default' },
    );
  });

  it('sends to dlq.connection when configured (separate DLQ cluster)', async () => {
    const { svc, kafkaClient } = makeService();
    await svc.handleFailure(
      [mkMsg()],
      new Error('boom'),
      { topic: 'orders-dlq', maxRetries: 0, connection: 'dlqCluster' },
      'orders',
      0,
      'clusterA',
      1,
    );
    expect(kafkaClient.sendBatch).toHaveBeenCalledWith(
      'orders-dlq',
      expect.anything(),
      { connection: 'dlqCluster' },
    );
  });

  it('returns retry, not a rejection, when the DLQ send fails', async () => {
    const { svc, kafkaClient } = makeService();
    kafkaClient.sendBatch.mockRejectedValue(new Error('dlq down'));
    const v = await svc.handleFailure(
      [mkMsg()],
      new Error('x'),
      { topic: 'd', maxRetries: 3, retryDelay: 1000 },
      't',
      0,
      undefined,
      4,
    );
    expect(v).toEqual({ action: 'retry', delayMs: 8000 });
  });
});

describe('DlqService.sendToDlq', () => {
  afterEach(() => jest.useRealTimers());

  it('sends value and key Buffers byte-for-byte', async () => {
    const { svc, kafkaClient } = makeService();
    await svc.sendToDlq(
      [
        mkMsg({
          value: Buffer.from([0xff, 0x00, 0xfe]),
          key: Buffer.from([0x80]),
        }),
      ],
      new Error('x'),
      { topic: 'd' },
      't',
      1,
    );
    const sent = kafkaClient.sendBatch.mock.calls[0][1][0];
    expect(Buffer.isBuffer(sent.value)).toBe(true);
    expect(sent.value.equals(Buffer.from([0xff, 0x00, 0xfe]))).toBe(true);
    expect(Buffer.isBuffer(sent.key)).toBe(true);
    expect(sent.key.equals(Buffer.from([0x80]))).toBe(true);
  });

  it('dead-letters several messages in one sendBatch call', async () => {
    const { svc, kafkaClient } = makeService();
    await svc.sendToDlq(
      [mkMsg({ offset: '1' }), mkMsg({ offset: '2' })],
      new Error('x'),
      { topic: 'd' },
      't',
      1,
    );
    expect(kafkaClient.sendBatch).toHaveBeenCalledTimes(1);
    const sent = kafkaClient.sendBatch.mock.calls[0][1];
    expect(sent).toHaveLength(2);
    for (const m of sent) {
      expect(m.headers['x-dlq-original-topic']).toBe('t');
    }
  });

  it('times out a send that never settles', async () => {
    jest.useFakeTimers();
    const { svc, kafkaClient } = makeService();
    kafkaClient.sendBatch.mockReturnValue(new Promise(() => {}));
    const p = svc.sendToDlq(
      [mkMsg()],
      new Error('x'),
      { topic: 'd', sendTimeout: 50 },
      't',
      1,
    );
    const assertion = expect(p).rejects.toThrow(
      'DLQ send timed out after 50ms',
    );
    jest.advanceTimersByTime(50);
    await assertion;
  });

  it('leaves no timer behind after a successful send', async () => {
    jest.useFakeTimers();
    const { svc } = makeService();
    await svc.sendToDlq([mkMsg()], new Error('x'), { topic: 'd' }, 't', 1);
    expect(jest.getTimerCount()).toBe(0);
  });
});
