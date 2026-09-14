import {
  deserializeMessage,
  deserializeMessageValue,
  deserializeHeaders,
} from './message.interface';

describe('deserializeMessageValue', () => {
  it('parses JSON, falls back to string, null for null', () => {
    expect(deserializeMessageValue(Buffer.from('{"a":1}'))).toEqual({ a: 1 });
    expect(deserializeMessageValue(Buffer.from('plain'))).toBe('plain');
    expect(deserializeMessageValue(null)).toBeNull();
  });
});

describe('deserializeHeaders', () => {
  it('converts buffers and arrays, skips null/undefined', () => {
    expect(
      deserializeHeaders({
        a: Buffer.from('x'), b: [Buffer.from('y')], c: null, d: 5,
      } as any),
    ).toEqual({ a: 'x', b: 'y', d: '5' });
  });
});

describe('deserializeMessage', () => {
  it('assembles the payload with topic and partition', () => {
    const p = deserializeMessage(
      { key: Buffer.from('k'), value: Buffer.from('"v"'), headers: {}, offset: '1', timestamp: '' } as any,
      'my-topic', 3,
    );
    expect(p).toMatchObject({ key: 'k', value: 'v', topic: 'my-topic', partition: 3, offset: '1' });
  });
});
