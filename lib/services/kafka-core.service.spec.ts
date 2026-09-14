import { KafkaCoreService } from './kafka-core.service';

jest.mock('@confluentinc/kafka-javascript', () => ({
  KafkaJS: {
    Kafka: jest.fn().mockImplementation(() => ({
      producer: jest
        .fn()
        .mockReturnValue({ connect: jest.fn(), disconnect: jest.fn() }),
    })),
    logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 },
  },
}));

const opts = { clientId: 'c', brokers: ['localhost:9092'] } as any;

describe('KafkaCoreService', () => {
  it('rejects missing clientId / brokers', () => {
    const core = new KafkaCoreService();
    expect(() => core.registerConnection({ brokers: ['b'] } as any)).toThrow(
      'clientId',
    );
    expect(() => core.registerConnection({ clientId: 'c' } as any)).toThrow(
      'brokers',
    );
  });

  it('skips duplicate connection names', () => {
    const core = new KafkaCoreService();
    core.registerConnection({ ...opts });
    core.registerConnection({ ...opts });
    expect(core.getConnectionNames()).toEqual(['default']);
  });

  it('throws for unknown connection', () => {
    expect(() => new KafkaCoreService().getKafka('nope')).toThrow('not found');
  });

  it('dedupes concurrent producer connects', async () => {
    const core = new KafkaCoreService();
    let resolveConnect!: () => void;
    const connect = jest.fn(
      () =>
        new Promise<void>((r) => {
          resolveConnect = r;
        }),
    );
    (core as any).connections.set('default', {
      producer: { connect },
      isProducerConnected: false,
      connectingPromise: null,
    });
    const p1 = core.connectProducer();
    const p2 = core.connectProducer();
    resolveConnect();
    await Promise.all([p1, p2]);
    expect(connect).toHaveBeenCalledTimes(1);
  });
});
