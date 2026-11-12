import { KafkaJS } from '@confluentinc/kafka-javascript';
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

  describe('producer options', () => {
    let core: KafkaCoreService;
    const lastProducerFactory = (): jest.Mock =>
      (KafkaJS.Kafka as unknown as jest.Mock).mock.results.at(-1)!.value
        .producer;

    beforeEach(() => {
      core = new KafkaCoreService();
    });

    it('wraps producer options in the kafkaJS block and maps numeric compression', () => {
      core.registerConnection({
        ...opts,
        producer: { acks: -1, timeout: 30000, compression: 1 },
      });
      expect(lastProducerFactory()).toHaveBeenCalledWith({
        kafkaJS: { acks: -1, timeout: 30000, compression: 'gzip' },
      });
    });

    it('passes codec names through', () => {
      core.registerConnection({ ...opts, producer: { compression: 'zstd' } });
      expect(lastProducerFactory()).toHaveBeenCalledWith({
        kafkaJS: { compression: 'zstd' },
      });
    });

    it('passes no producer config when none is given', () => {
      core.registerConnection({ ...opts });
      expect(lastProducerFactory().mock.calls[0][0]).toBeUndefined();
    });

    it('rejects an unknown compression value at registration', () => {
      expect(() =>
        core.registerConnection({
          ...opts,
          name: 'a',
          producer: { compression: 7 as any },
        }),
      ).toThrow('Kafka connection "a": unsupported compression 7');
    });
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
