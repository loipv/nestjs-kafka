import { DlqService } from './dlq.service';

const mkMsg = () =>
  ({
    offset: '5',
    value: Buffer.from('{}'),
    key: null,
    headers: {},
    timestamp: '',
  }) as any;

function makeService(circuitOpen = false) {
  const kafkaClient = { send: jest.fn().mockResolvedValue(undefined) };
  const metrics = {
    recordHandlerRetry: jest.fn(),
    recordSentToDlq: jest.fn(),
    recordFinalFailure: jest.fn(),
  };
  const circuitBreaker = {
    canExecute: jest.fn().mockReturnValue(!circuitOpen),
    recordSuccess: jest.fn(),
    recordFailure: jest.fn(),
    getState: jest.fn().mockReturnValue('OPEN'),
    reset: jest.fn(),
  };
  const svc = new DlqService(
    kafkaClient as any,
    metrics as any,
    circuitBreaker as any,
  );
  return { svc, kafkaClient, metrics, circuitBreaker };
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

  it('completes with drop when circuit breaker is open', async () => {
    const { svc, metrics } = makeService(true);
    const v = await svc.handleFailure(
      mkMsg(),
      new Error('x'),
      { topic: 'dlq', maxRetries: 0 },
      't',
      0,
      'default',
      1,
    );
    expect(v).toEqual({ action: 'complete' });
    expect(metrics.recordFinalFailure).toHaveBeenCalledWith('t', false);
  });

  it('propagates DLQ send errors → infra crash path (auto-restart), no data loss', async () => {
    const kafkaClient = {
      send: jest.fn().mockRejectedValue(new Error('dlq broker down')),
    };
    const svc = new DlqService(
      kafkaClient as any,
      { recordHandlerRetry: jest.fn() } as any,
      {
        canExecute: jest.fn().mockReturnValue(true),
        recordSuccess: jest.fn(),
        recordFailure: jest.fn(),
        getState: jest.fn(),
      } as any,
    );
    await expect(
      svc.handleFailure(
        mkMsg(),
        new Error('x'),
        { topic: 'dlq', maxRetries: 0 },
        't',
        0,
        'default',
        1,
      ),
    ).rejects.toThrow('dlq broker down');
  });
});
