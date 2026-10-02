import { DlqRetryService, DLQ_RETRY_HEADERS } from './dlq-retry.service';

function makeService() {
  const consumerMock = {
    connect: jest.fn(),
    subscribe: jest.fn(),
    run: jest.fn(),
    disconnect: jest.fn(),
  };
  const kafkaCore = {
    getKafka: jest
      .fn()
      .mockReturnValue({ consumer: jest.fn().mockReturnValue(consumerMock) }),
  };
  const kafkaClient = { send: jest.fn().mockResolvedValue(undefined) };
  const metrics = {
    recordReprocessAttempt: jest.fn(),
    recordReprocessSuccess: jest.fn(),
    recordFinalFailure: jest.fn(),
  };
  const svc = new DlqRetryService(
    kafkaCore as any,
    kafkaClient as any,
    metrics as any,
  );
  return { svc, kafkaClient, metrics, kafkaCore };
}

const mkMetadata = (dlqTopic: string, retryOpts: any = {}) =>
  ({
    topic: 'src',
    connection: 'default',
    options: {
      dlq: {
        topic: dlqTopic,
        retry: { enabled: true, delay: 10, ...retryOpts },
      },
    },
    target: {},
    methodName: 'h',
  }) as any;

describe('DlqRetryService', () => {
  it('appends -dlq suffix on groupId collision with an original consumer group', () => {
    const { svc } = makeService();
    svc.registerOriginalGroupId('default', 't-dlq-retry-consumer');
    svc.registerDlqRetryConsumer(mkMetadata('t-dlq'), async () => {});
    expect(
      (svc as any).dlqConsumerGroups.has('default:t-dlq-retry-consumer-dlq'),
    ).toBe(true);
  });

  it('consumes the DLQ from dlq.connection when configured', () => {
    const { svc, kafkaCore } = makeService();
    const md = mkMetadata('t-dlq');
    md.options.connection = 'clusterA';
    md.options.dlq.connection = 'dlqCluster';
    svc.registerDlqRetryConsumer(md, async () => {});
    expect(kafkaCore.getKafka).toHaveBeenCalledWith('dlqCluster');
    expect(
      (svc as any).dlqConsumerGroups.get('dlqCluster:t-dlq-retry-consumer')
        ?.connection,
    ).toBe('dlqCluster');
  });

  it('reprocess failure sends message back to DLQ with incremented count', async () => {
    const { svc, kafkaClient } = makeService();
    const handler = {
      dlqTopic: 't-dlq',
      retryOptions: { maxRetries: 3, delay: 10 },
      originalTopic: 'src',
      originalHandler: jest.fn().mockRejectedValue(new Error('x')),
      originalOptions: {},
    };
    const msg = {
      offset: '1',
      key: null,
      value: Buffer.from('{}'),
      timestamp: '',
      headers: { [DLQ_RETRY_HEADERS.REPROCESS_COUNT]: '1' },
    } as any;
    await (svc as any).handleDlqMessage(msg, 0, handler, 'default', 't-dlq');
    expect(kafkaClient.send).toHaveBeenCalledWith(
      't-dlq',
      expect.objectContaining({
        headers: expect.objectContaining({
          [DLQ_RETRY_HEADERS.REPROCESS_COUNT]: '2',
        }),
      }),
      { connection: 'default' },
    );
  });

  it('routes to final DLQ when max reprocesses exceeded', async () => {
    const { svc, kafkaClient, metrics } = makeService();
    const handler = {
      dlqTopic: 't-dlq',
      retryOptions: { maxRetries: 2, delay: 10, finalDlqTopic: 't-final' },
      originalTopic: 'src',
      originalHandler: jest.fn(),
      originalOptions: {},
    };
    const msg = {
      offset: '1',
      key: null,
      value: Buffer.from('{}'),
      timestamp: '',
      headers: { [DLQ_RETRY_HEADERS.REPROCESS_COUNT]: '2' },
    } as any;
    await (svc as any).handleDlqMessage(msg, 0, handler, 'default', 't-dlq');
    expect(kafkaClient.send).toHaveBeenCalledWith(
      't-final',
      expect.objectContaining({
        headers: expect.objectContaining({
          'x-final-dlq-reason': 'max-reprocess-exceeded',
        }),
      }),
      { connection: 'default' },
    );
    expect(metrics.recordFinalFailure).toHaveBeenCalledWith('t-dlq', true);
  });
});
