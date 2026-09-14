import { KafkaClient } from './kafka-client.service';

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

  describe('onApplicationShutdown', () => {
    it('still disconnects producers when final flush fails', async () => {
      const core = {
        isProducerConnected: jest.fn().mockReturnValue(false),
        disconnectAll: jest.fn().mockResolvedValue(undefined),
        getProducer: jest.fn().mockReturnValue({
          send: jest.fn().mockRejectedValue(new Error('broker down')),
        }),
      } as any;
      const client = new KafkaClient(core);
      (client as any).batchBuffers.set(
        'default',
        new Map([['t', [{ value: 'x' }]]]),
      );

      await expect(client.onApplicationShutdown()).resolves.toBeUndefined();
      expect(core.disconnectAll).toHaveBeenCalledTimes(1);
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

    it('sendQueued buffers below threshold, timer flushes at ~100ms', async () => {
      const producer = { send: jest.fn().mockResolvedValue(undefined) };
      const client = new KafkaClient(mkCore(producer) as any);
      await client.sendQueued('t', { value: 'a' });
      expect(producer.send).not.toHaveBeenCalled();
      await new Promise((r) => setTimeout(r, 150));
      expect(producer.send).toHaveBeenCalledWith({
        topic: 't',
        messages: [expect.objectContaining({ value: 'a' })],
      });
    });

    it('sendQueued flushes immediately at 100 messages', async () => {
      const producer = { send: jest.fn().mockResolvedValue(undefined) };
      const client = new KafkaClient(mkCore(producer) as any);
      for (let i = 0; i < 100; i++)
        await client.sendQueued('t', { value: `m${i}` });
      expect(producer.send).toHaveBeenCalledTimes(1);
      expect(producer.send.mock.calls[0][0].messages).toHaveLength(100);
    });
  });
});
