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

    it('drops producer options set to undefined', () => {
      core.registerConnection({
        ...opts,
        producer: { acks: -1, timeout: undefined, idempotent: undefined },
      });
      const config = lastProducerFactory().mock.calls[0][0];
      expect(Object.keys(config.kafkaJS)).toEqual(['acks']);
    });

    it('replaces a producer whose connect failed so the next connect retries', async () => {
      const failed = {
        connect: jest
          .fn()
          .mockRejectedValue(new Error('broker transport failure')),
      };
      const fresh = { connect: jest.fn().mockResolvedValue(undefined) };
      const producerFactory = jest
        .fn()
        .mockReturnValueOnce(failed)
        .mockReturnValueOnce(fresh);
      (KafkaJS.Kafka as unknown as jest.Mock).mockImplementationOnce(() => ({
        producer: producerFactory,
      }));

      core.registerConnection({ ...opts, producer: { acks: -1 } });
      await expect(core.connectProducer()).rejects.toThrow(
        'broker transport failure',
      );
      await core.connectProducer();

      expect(fresh.connect).toHaveBeenCalledTimes(1);
      expect(core.getProducer()).toBe(fresh);
      expect(core.isProducerConnected()).toBe(true);
      expect(producerFactory).toHaveBeenNthCalledWith(2, {
        kafkaJS: { acks: -1 },
      });
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

  describe('TLS and rdkafka passthrough', () => {
    const lastKafkaConfig = () =>
      (KafkaJS.Kafka as unknown as jest.Mock).mock.calls.at(-1)![0];

    it('maps an ssl object to librdkafka keys', () => {
      new KafkaCoreService().registerConnection({
        ...opts,
        ssl: {
          ca: [Buffer.from('A'), 'B'],
          cert: 'C',
          key: Buffer.from('K'),
          passphrase: 'p',
          rejectUnauthorized: false,
        },
      });
      const config = lastKafkaConfig();
      expect(config.kafkaJS.ssl).toBe(true);
      expect(config['ssl.ca.pem']).toBe('A\nB');
      expect(config['ssl.certificate.pem']).toBe('C');
      expect(config['ssl.key.pem']).toBe('K');
      expect(config['ssl.key.password']).toBe('p');
      expect(config['enable.ssl.certificate.verification']).toBe(false);
    });

    it('ssl: true adds no librdkafka ssl keys', () => {
      new KafkaCoreService().registerConnection({ ...opts, ssl: true });
      const config = lastKafkaConfig();
      expect(config.kafkaJS.ssl).toBe(true);
      expect(Object.keys(config).filter((k) => k.startsWith('ssl.'))).toEqual(
        [],
      );
    });

    it('rejects an unknown ssl option', () => {
      expect(() =>
        new KafkaCoreService().registerConnection({
          ...opts,
          name: 'a',
          ssl: { servername: 'x' } as any,
        }),
      ).toThrow(
        'Kafka connection "a": unsupported ssl option "servername"; pass librdkafka keys through rdkafka',
      );
    });

    it('rdkafka overrides generated keys', () => {
      new KafkaCoreService().registerConnection({
        ...opts,
        ssl: { ca: 'A' },
        rdkafka: { 'ssl.ca.pem': 'Z', 'socket.keepalive.enable': true },
      });
      const config = lastKafkaConfig();
      expect(config['ssl.ca.pem']).toBe('Z');
      expect(config['socket.keepalive.enable']).toBe(true);
    });
  });

  it('connects every producer at bootstrap without throwing', async () => {
    const failing = { connect: jest.fn().mockRejectedValue(new Error('down')) };
    const ok = { connect: jest.fn().mockResolvedValue(undefined) };
    (KafkaJS.Kafka as unknown as jest.Mock)
      .mockImplementationOnce(() => ({ producer: () => failing }))
      .mockImplementationOnce(() => ({ producer: () => ok }));
    const core = new KafkaCoreService();
    const warn = jest
      .spyOn((core as any).logger, 'warn')
      .mockImplementation(() => {});
    core.registerConnection({ ...opts });
    core.registerConnection({ ...opts, name: 'analytics' });

    expect(core.onApplicationBootstrap()).toBeUndefined();
    await new Promise((r) => setImmediate(r));

    expect(failing.connect).toHaveBeenCalledTimes(1);
    expect(ok.connect).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(
      'Producer for connection "default" could not connect at startup: Error: down',
    );
  });

  it('disconnectAll disconnects a producer whose connect is still in flight, without waiting for it', async () => {
    const producer = {
      connect: jest.fn(() => new Promise<void>(() => {})), // unreachable broker
      disconnect: jest.fn().mockResolvedValue(undefined),
    };
    (KafkaJS.Kafka as unknown as jest.Mock).mockImplementationOnce(() => ({
      producer: () => producer,
    }));
    const core = new KafkaCoreService();
    core.registerConnection({ ...opts });

    core.onApplicationBootstrap(); // connect starts and never settles
    await core.disconnectAll();

    expect(producer.disconnect).toHaveBeenCalledTimes(1);
    expect(core.isProducerConnected()).toBe(false);
  });

  it('records consumer group statuses', () => {
    const core = new KafkaCoreService();
    core.setConsumerStatus('default:g', 'starting');
    core.setConsumerStatus('default:g', 'running');
    expect(core.getConsumerStatuses()).toEqual({ 'default:g': 'running' });
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
