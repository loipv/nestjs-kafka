import { ModuleMetadata, Type } from '@nestjs/common';
import { SASLOptions } from 'kafkajs';

export interface RetryOptions {
  initialRetryTime?: number;
  retries?: number;
  maxRetryTime?: number;
  factor?: number;
  multiplier?: number;
}

export interface ProducerConfig {
  createPartitioner?: () => any;
  retry?: RetryOptions;
  metadataMaxAge?: number;
  allowAutoTopicCreation?: boolean;
  idempotent?: boolean;
  transactionalId?: string;
  transactionTimeout?: number;
  maxInFlightRequests?: number;
}

export const DEFAULT_KAFKA_CONNECTION: string = 'default';

export interface KafkaModuleOptions {
  /** Connection name for multi-connection support. Default: 'default' */
  name?: string;
  clientId: string;
  brokers: string[] | (() => string[] | Promise<string[]>);

  ssl?: boolean | object;
  sasl?: SASLOptions;

  connectionTimeout?: number;
  requestTimeout?: number;
  enforceRequestTimeout?: boolean;

  retry?: RetryOptions;

  producer?: ProducerConfig;

  defaultConsumerGroupId?: string;

  logLevel?: 'NOTHING' | 'ERROR' | 'WARN' | 'INFO' | 'DEBUG';
}

export interface KafkaModuleAsyncOptions extends Pick<
  ModuleMetadata,
  'imports'
> {
  /** Connection name for multi-connection support. Default: 'default' */
  name?: string;
  useFactory?: (
    ...args: any[]
  ) => Promise<KafkaModuleOptions> | KafkaModuleOptions;
  inject?: any[];
  useClass?: Type<KafkaOptionsFactory>;
  useExisting?: Type<KafkaOptionsFactory>;
  global?: boolean;
}

export interface KafkaOptionsFactory {
  createKafkaOptions(): Promise<KafkaModuleOptions> | KafkaModuleOptions;
}

export const KAFKA_MODULE_OPTIONS = Symbol('KAFKA_MODULE_OPTIONS');

/** Get the injection token for a specific Kafka connection */
export function getKafkaOptionsToken(name?: string): string {
  return `KAFKA_OPTIONS_${name || DEFAULT_KAFKA_CONNECTION}`;
}

/** Get the injection token for a specific Kafka core service */
export function getKafkaCoreToken(name?: string): string {
  return `KAFKA_CORE_${name || DEFAULT_KAFKA_CONNECTION}`;
}

/** Get the injection token for a specific Kafka client */
export function getKafkaClientToken(name?: string): string {
  return `KAFKA_CLIENT_${name || DEFAULT_KAFKA_CONNECTION}`;
}
