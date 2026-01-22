import { ModuleMetadata, Type } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';

type SASLOptions = KafkaJS.SASLOptions;

export interface RetryOptions {
  initialRetryTime?: number;
  retries?: number;
  maxRetryTime?: number;
}

export interface ProducerConfig {
  retry?: RetryOptions;
  metadataMaxAge?: number;
  allowAutoTopicCreation?: boolean;
  idempotent?: boolean;
  transactionalId?: string;
  transactionTimeout?: number;
  maxInFlightRequests?: number;
  /**
   * Number of acknowledgments the producer requires before considering a request complete.
   * -1: All in-sync replicas (default)
   * 0: No acknowledgment
   * 1: Leader acknowledgment only
   * Note: In confluent-kafka-javascript, this is set at producer level, not per-send
   */
  acks?: -1 | 0 | 1;
  /**
   * Compression type for messages.
   * 0: None, 1: GZIP, 2: Snappy, 3: LZ4, 4: ZSTD
   * Note: In confluent-kafka-javascript, this is set at producer level, not per-send
   */
  compression?: 0 | 1 | 2 | 3 | 4;
  /**
   * Maximum time in ms to wait for the producer to send a message.
   * Note: In confluent-kafka-javascript, this is set at producer level, not per-send
   */
  timeout?: number;
}

export interface TracingOptions {
  /** Enable OpenTelemetry tracing. Default: false */
  enabled?: boolean;
  /** Custom tracer name. Default: '@loipv/nestjs-kafka' */
  tracerName?: string;
  /** Custom tracer version. */
  tracerVersion?: string;
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

  /** OpenTelemetry tracing configuration */
  tracing?: TracingOptions;
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
