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

export interface KafkaModuleOptions {
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
