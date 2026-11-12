import { ConnectionBoundClient, KafkaClient } from './kafka-client.service';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript', () => ({
  KafkaJS: {
    Kafka: jest.fn(),
    logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 },
  },
}));

describe('KafkaClient', () => {
  const client = new KafkaClient({} as any, undefined);
  const serialize = (msg: any) => (client as any).serializeMessage(msg);

  describe('serializeMessage', () => {
    it('passes Buffer values through unchanged', () => {
      const buf = Buffer.from('binary-data');
      const result = serialize({ value: buf });
      expect(result.value).toBe(buf);
    });

    it('JSON-stringifies plain objects', () => {
      expect(serialize({ value: { a: 1 } }).value).toBe('{"a":1}');
    });

    it('stringifies primitives', () => {
      expect(serialize({ value: 42 }).value).toBe('42');
    });

    it('keeps null/undefined as null', () => {
      expect(serialize({ value: null }).value).toBeNull();
      expect(serialize({ value: undefined }).value).toBeNull();
    });

    it('keeps key 0, empty-string and Buffer keys', () => {
      const buf = Buffer.from([0x80, 0xff]);
      expect(serialize({ key: '', value: 'v' }).key).toBe('');
      expect(serialize({ key: buf, value: 'v' }).key).toBe(buf);
      expect(serialize({ key: null, value: 'v' }).key).toBeNull();
      expect(serialize({ value: 'v' }).key).toBeNull();
      expect(serialize({ key: 0, value: 'v' }).key).toBe('0');
    });
  });

  describe('isHealthy', () => {
    it('reports producer connection state, not registration', () => {
      const core = {
        isProducerConnected: jest.fn().mockReturnValue(true),
      } as any;
      expect(new KafkaClient(core).isHealthy()).toBe(true);
      expect(core.isProducerConnected).toHaveBeenCalledWith(undefined);
    });

    it('returns false when producer never connected (lazy connect)', () => {
      const core = {
        isProducerConnected: jest.fn().mockReturnValue(false),
      } as any;
      expect(new KafkaClient(core).isHealthy()).toBe(false);
    });
  });

  describe('send paths', () => {
    const mkCore = (producer: any) => ({
      connectProducer: jest.fn(),
      getProducer: jest.fn().mockReturnValue(producer),
      isProducerConnected: jest.fn().mockReturnValue(false),
      disconnectAll: jest.fn(),
    });

    it('sendBatch serializes and sends all messages', async () => {
      const producer = { send: jest.fn().mockResolvedValue(undefined) };
      const client = new KafkaClient(mkCore(producer) as any);
      await client.sendBatch('t', [{ value: { a: 1 } }, { value: 'x' }]);
      expect(producer.send).toHaveBeenCalledWith({
        topic: 't',
        messages: [
          {
            key: null,
            value: '{"a":1}',
            headers: {},
            partition: undefined,
            timestamp: undefined,
          },
          {
            key: null,
            value: 'x',
            headers: {},
            partition: undefined,
            timestamp: undefined,
          },
        ],
      });
    });
  });

  describe('connections and shutdown', () => {
    const mkCore = () => ({
      connectProducer: jest.fn().mockResolvedValue(undefined),
      getProducer: jest
        .fn()
        .mockReturnValue({ send: jest.fn().mockResolvedValue(undefined) }),
      disconnectAll: jest.fn().mockResolvedValue(undefined),
    });
    let kafkaCore: ReturnType<typeof mkCore>;
    let client: KafkaClient;

    beforeEach(() => {
      kafkaCore = mkCore();
      client = new KafkaClient(kafkaCore as any);
    });

    it('onApplicationShutdown disconnects producers', async () => {
      await client.onApplicationShutdown();
      expect(kafkaCore.disconnectAll).toHaveBeenCalledTimes(1);
    });

    it('send uses options.connection', async () => {
      await client.send('t', { value: 'v' }, { connection: 'analytics' });
      expect(kafkaCore.connectProducer).toHaveBeenCalledWith('analytics');
      expect(kafkaCore.getProducer).toHaveBeenCalledWith('analytics');
    });

    it('ConnectionBoundClient forwards its connection', async () => {
      await new ConnectionBoundClient(client, 'analytics').sendBatch('t', [
        { value: 'v' },
      ]);
      expect(kafkaCore.getProducer).toHaveBeenCalledWith('analytics');
    });

    it('no longer exposes sendQueued or forConnection', () => {
      const bound = new ConnectionBoundClient(client, 'analytics');
      expect((client as any).sendQueued).toBeUndefined();
      expect((client as any).forConnection).toBeUndefined();
      expect((bound as any).sendQueued).toBeUndefined();
    });
  });
});
