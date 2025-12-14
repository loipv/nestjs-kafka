import { IHeaders, KafkaMessage } from 'kafkajs';

export interface KafkaMessagePayload<T = any> {
  key?: string | null;
  value: T;
  headers?: IHeaders;
  partition?: number;
  timestamp?: string;
  offset?: string;
  topic?: string;
}

/**
 * Deserialize a Kafka message value
 * - Tries JSON.parse first
 * - Falls back to string conversion
 * - Returns null for null values
 */
export function deserializeMessageValue(value: Buffer | null): any {
  if (value === null) {
    return null;
  }

  const stringValue = value.toString('utf-8');

  try {
    return JSON.parse(stringValue);
  } catch {
    return stringValue;
  }
}

/**
 * Deserialize a Kafka message key
 * - Converts Buffer to string
 * - Returns null for null values
 */
export function deserializeMessageKey(
  key: Buffer | string | null | undefined,
): string | null {
  if (key === null || key === undefined) {
    return null;
  }
  if (Buffer.isBuffer(key)) {
    return key.toString('utf-8');
  }
  return key;
}

/**
 * Transform a raw Kafka message into a deserialized payload
 */
export function deserializeMessage<T = any>(
  message: KafkaMessage,
  topic?: string,
  partition?: number,
): KafkaMessagePayload<T> {
  return {
    key: deserializeMessageKey(message.key),
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    value: deserializeMessageValue(message.value),
    headers: message.headers,
    timestamp: message.timestamp,
    offset: message.offset,
    topic,
    partition,
  };
}

export interface ProducerMessage {
  key?: string | Buffer | null;
  value: string | Buffer | object | null;
  headers?: IHeaders;
  partition?: number;
  timestamp?: string;
}

export interface SendOptions {
  acks?: -1 | 0 | 1;
  timeout?: number;
  compression?: 0 | 1 | 2 | 3 | 4;
}

export interface GroupedBatch<T = any> {
  key: string;
  messages: KafkaMessagePayload<T>[];
}
