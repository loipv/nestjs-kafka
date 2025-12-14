import { IHeaders } from 'kafkajs';

export interface KafkaMessagePayload<T = any> {
  key?: string | Buffer | null;
  value: T;
  headers?: IHeaders;
  partition?: number;
  timestamp?: string;
  offset?: string;
  topic?: string;
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
