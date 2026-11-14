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

  it('returns what JSON.parse-or-string returns for every input class', () => {
    const reference = (s: string): unknown => {
      try {
        return JSON.parse(s);
      } catch {
        return s;
      }
    };
    const inputs = [
      'hello',
      '',
      '   ',
      '\t\n\r ',
      '42',
      '-1.5',
      '1e3',
      '0',
      'true',
      'false',
      'null',
      ' {"a":1}',
      '\n[1,2]',
      '"quoted"',
      '{not json',
      'tru',
      'nul',
      '-',
      '0x10',
      'NaN',
      ' {"a":1}',
      '﻿{"a":1}',
      'héllo wörld',
      '[',
      '{}',
      '[]',
    ];
    for (const s of inputs) {
      expect([s, deserializeMessageValue(Buffer.from(s, 'utf-8'))]).toEqual([
        s,
        reference(s),
      ]);
    }
  });

  it('does not call JSON.parse for a value that cannot start a JSON document', () => {
    const parse = jest.spyOn(JSON, 'parse');
    try {
      expect(deserializeMessageValue(Buffer.from('order 7 processed'))).toBe(
        'order 7 processed',
      );
      expect(parse).not.toHaveBeenCalled();
    } finally {
      parse.mockRestore();
    }
  });
});

describe('deserializeHeaders', () => {
  it('converts buffers and arrays, skips null/undefined', () => {
    expect(
      deserializeHeaders({
        a: Buffer.from('x'),
        b: [Buffer.from('y')],
        c: null,
        d: 5,
      } as any),
    ).toEqual({ a: 'x', b: 'y', d: '5' });
  });
});

describe('deserializeMessage', () => {
  it('assembles the payload with topic and partition', () => {
    const p = deserializeMessage(
      {
        key: Buffer.from('k'),
        value: Buffer.from('"v"'),
        headers: {},
        offset: '1',
        timestamp: '',
      } as any,
      'my-topic',
      3,
    );
    expect(p).toMatchObject({
      key: 'k',
      value: 'v',
      topic: 'my-topic',
      partition: 3,
      offset: '1',
    });
  });
});
