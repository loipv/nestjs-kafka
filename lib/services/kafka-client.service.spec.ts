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
});
