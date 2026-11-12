import { DlqService } from './dlq.service';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript', () => ({
  KafkaJS: {
    Kafka: jest.fn(),
    logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 },
  },
}));

const mkMsg = () =>
  ({
    offset: '5',
    value: Buffer.from('{}'),
    key: null,
    headers: {},
    timestamp: '',
  }) as any;

function makeService() {
  const kafkaClient = { send: jest.fn().mockResolvedValue(undefined) };
  const svc = new DlqService(kafkaClient as any);
  return { svc, kafkaClient };
}

describe('DlqService.handleFailure (verdict API)', () => {
  it('returns retry verdict with capped exponential delay', async () => {
    const { svc } = makeService();
    const v = await svc.handleFailure(
      mkMsg(),
      new Error('x'),
      { topic: 'dlq' },
      't',
      0,
      'default',
      1,
    );
    expect(v).toEqual({ action: 'retry', delayMs: 1000 });
  });

  it('caps delay at MAX_RETRY_DELAY_MS', async () => {
    const { svc } = makeService();
    const v = await svc.handleFailure(
      mkMsg(),
      new Error('x'),
      { topic: 'dlq', retryDelay: 60000, retryBackoffMultiplier: 3 },
      't',
      0,
      'default',
      2,
    );
    expect(v).toEqual({ action: 'retry', delayMs: 30000 });
  });

  it('sends to DLQ and completes when retries exhausted', async () => {
    const { svc, kafkaClient } = makeService();
    const v = await svc.handleFailure(
      mkMsg(),
      new Error('boom'),
      { topic: 'orders-dlq', maxRetries: 3 },
      'orders',
      0,
      'default',
      4,
    );
    expect(v).toEqual({ action: 'complete' });
    expect(kafkaClient.send).toHaveBeenCalledWith(
      'orders-dlq',
      expect.objectContaining({
        headers: expect.objectContaining({ 'x-dlq-original-topic': 'orders' }),
      }),
      { connection: 'default' },
    );
  });

  it('sends to dlq.connection when configured (separate DLQ cluster)', async () => {
    const { svc, kafkaClient } = makeService();
    await svc.handleFailure(
      mkMsg(),
      new Error('boom'),
      { topic: 'orders-dlq', maxRetries: 0, connection: 'dlqCluster' },
      'orders',
      0,
      'clusterA',
      1,
    );
    expect(kafkaClient.send).toHaveBeenCalledWith(
      'orders-dlq',
      expect.anything(),
      { connection: 'dlqCluster' },
    );
  });

  it('rejects on every DLQ send failure and never completes', async () => {
    const { svc, kafkaClient } = makeService();
    kafkaClient.send.mockRejectedValue(new Error('dlq down'));
    for (let i = 0; i < 10; i++) {
      await expect(
        svc.handleFailure(
          mkMsg(),
          new Error('x'),
          { topic: 'd', maxRetries: 0 },
          't',
          0,
          undefined,
          1,
        ),
      ).rejects.toThrow('dlq down');
    }
  });
});
