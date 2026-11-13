import { ModuleMetadata, Type } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';

type SASLOptions = KafkaJS.SASLOptions;

/** Lifecycle state of one consumer group, keyed `<connection>:<groupId>` */
export type ConsumerGroupStatus = 'starting' | 'running' | 'failed' | 'stopped';

export interface RetryOptions {
  initialRetryTime?: number;
  retries?: number;
  maxRetryTime?: number;
}

export type CompressionCodec = 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd';

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
   * Compression codec for messages: a codec name, or its number
   * (0: none, 1: gzip, 2: snappy, 3: lz4, 4: zstd).
   * Note: In confluent-kafka-javascript, this is set at producer level, not per-send
   */
  compression?: 0 | 1 | 2 | 3 | 4 | CompressionCodec;
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
  /** Custom tracer version. Default: '0.0.1' */
  tracerVersion?: string;
}

export const DEFAULT_KAFKA_CONNECTION: string = 'default';

/**
 * TLS material, applied as librdkafka `ssl.*` keys. For any other TLS setting
 * pass the librdkafka key through `rdkafka`.
 */
export interface SslOptions {
  /** CA certificate(s), PEM → `ssl.ca.pem` (several are joined) */
  ca?: string | Buffer | Array<string | Buffer>;
  /** Client certificate, PEM → `ssl.certificate.pem` */
  cert?: string | Buffer;
  /** Client private key, PEM → `ssl.key.pem` */
  key?: string | Buffer;
  /** Private key password → `ssl.key.password` */
  passphrase?: string;
  /** false → `enable.ssl.certificate.verification: false` */
  rejectUnauthorized?: boolean;
}

export interface KafkaModuleOptions {
  /**
   * Connection name; call forRoot() once per name. A second registration of
   * the same name with different options fails at startup. Default: 'default'
   */
  name?: string;
  clientId: string;
  brokers: string[];

  /** `true` enables TLS with system CAs; an object also sets TLS material */
  ssl?: boolean | SslOptions;
  sasl?: SASLOptions;
  /**
   * Raw librdkafka properties for this connection's producer, consumers and
   * admin clients. They override the keys built from `ssl` and the kafkaJS
   * options, but not the consumer keys the library sets itself
   * (`js.consumer.max.batch.size`, `rebalance_cb`). Consumer-only keys also
   * reach the producer and admin clients, where librdkafka warns and ignores them.
   * @example { 'socket.keepalive.enable': true, 'ssl.endpoint.identification.algorithm': 'none' }
   */
  rdkafka?: Record<string, string | number | boolean>;

  connectionTimeout?: number;
  requestTimeout?: number;
  enforceRequestTimeout?: boolean;

  retry?: RetryOptions;

  producer?: ProducerConfig;

  logLevel?: 'NOTHING' | 'ERROR' | 'WARN' | 'INFO' | 'DEBUG';

  /** OpenTelemetry tracing configuration */
  tracing?: TracingOptions;
}

export interface KafkaModuleAsyncOptions extends Pick<
  ModuleMetadata,
  'imports'
> {
  /** Connection name (see KafkaModuleOptions.name). Default: 'default' */
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

/** Get the injection token for a specific Kafka connection */
export function getKafkaOptionsToken(name?: string): string {
  return `KAFKA_OPTIONS_${name || DEFAULT_KAFKA_CONNECTION}`;
}

/** Get the injection token for a specific Kafka client */
export function getKafkaClientToken(name?: string): string {
  return `KAFKA_CLIENT_${name || DEFAULT_KAFKA_CONNECTION}`;
}
