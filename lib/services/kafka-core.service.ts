import { isDeepStrictEqual } from 'node:util';
import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  CompressionCodec,
  ConsumerGroupStatus,
  KafkaModuleOptions,
  SslOptions,
  ProducerConfig,
  DEFAULT_KAFKA_CONNECTION,
} from '../interfaces';

/** Indexed by the numeric compression value (0..4). */
const COMPRESSION_CODECS: readonly CompressionCodec[] = [
  'none',
  'gzip',
  'snappy',
  'lz4',
  'zstd',
];

type Kafka = InstanceType<typeof KafkaJS.Kafka>;
type Producer = KafkaJS.Producer;
type Admin = KafkaJS.Admin;
type ProducerConstructorConfig = Parameters<Kafka['producer']>[0];

interface KafkaConnection {
  kafka: Kafka;
  producer: Producer;
  /** Kept so a producer whose connect failed can be rebuilt identically */
  producerConfig: ProducerConstructorConfig;
  options: KafkaModuleOptions;
  isProducerConnected: boolean;
  connectingPromise: Promise<void> | null;
}

/** How long shutdown waits for a producer connect still in flight (the client's own disconnect timeout) */
const IN_FLIGHT_CONNECT_WAIT_MS = 5000;

@Injectable()
export class KafkaCoreService implements OnApplicationBootstrap {
  private readonly logger = new Logger(KafkaCoreService.name);
  private connections = new Map<string, KafkaConnection>();
  private consumerStatuses = new Map<string, ConsumerGroupStatus>();
  /** One admin per connection for health probes: the client and its connect */
  private admins = new Map<string, { admin: Admin; ready: Promise<Admin> }>();
  /** Set by disconnectAll: no background reconnect after shutdown */
  private isShutDown = false;

  /**
   * Register a new Kafka connection
   */
  registerConnection(options: KafkaModuleOptions): void {
    const name = options.name || DEFAULT_KAFKA_CONNECTION;

    if (!options.clientId || options.clientId.trim() === '') {
      throw new Error(
        `Kafka connection "${name}": clientId is required and cannot be empty`,
      );
    }

    const brokers = options.brokers;
    if (!brokers || brokers.length === 0) {
      throw new Error(
        `Kafka connection "${name}": brokers is required and cannot be empty`,
      );
    }

    // The same options registered twice (e.g. one config object passed to two
    // forRoot() calls) are harmless; different options under one name conflict.
    const existing = this.connections.get(name);
    if (existing) {
      if (isDeepStrictEqual(existing.options, options)) return;
      const keys = new Set([
        ...Object.keys(existing.options),
        ...Object.keys(options),
      ]);
      const differing = [...keys].filter(
        (key) =>
          !isDeepStrictEqual(
            existing.options[key as keyof KafkaModuleOptions],
            options[key as keyof KafkaModuleOptions],
          ),
      );
      throw new Error(
        `Kafka connection "${name}" is already registered with different options (${differing.join(', ')})`,
      );
    }

    // librdkafka would override kafkaJS.autoCommit, and the library only
    // commits itself when it knows auto-commit is off.
    if (options.rdkafka && 'enable.auto.commit' in options.rdkafka) {
      throw new Error(
        `Kafka connection "${name}": set autoCommit on the consumer instead of rdkafka "enable.auto.commit"`,
      );
    }

    // Build Kafka config with only defined values to avoid undefined property issues
    const kafkaConfig: KafkaJS.KafkaConfig = {
      clientId: options.clientId,
      brokers: options.brokers,
      logLevel: this.mapLogLevel(options.logLevel),
      // Use default library logger - custom logger can cause issues with internal log formatting
    };

    // Add optional settings only if defined
    let sslKeys: Record<string, string | boolean> = {};
    if (options.ssl !== undefined) {
      kafkaConfig.ssl = !!options.ssl;
      if (typeof options.ssl === 'object' && options.ssl !== null) {
        sslKeys = this.toSslKeys(name, options.ssl);
      }
    }
    if (options.sasl !== undefined) {
      kafkaConfig.sasl = options.sasl;
    }
    if (options.connectionTimeout !== undefined) {
      kafkaConfig.connectionTimeout = options.connectionTimeout;
    }
    if (options.requestTimeout !== undefined) {
      kafkaConfig.requestTimeout = options.requestTimeout;
    }
    if (options.enforceRequestTimeout !== undefined) {
      kafkaConfig.enforceRequestTimeout = options.enforceRequestTimeout;
    }
    if (options.retry !== undefined) {
      kafkaConfig.retry = options.retry;
    }

    // Top-level keys are librdkafka properties shared by the producer,
    // consumers and admin clients of this connection; `rdkafka` wins.
    const kafka = new KafkaJS.Kafka({
      kafkaJS: kafkaConfig,
      ...sslKeys,
      ...options.rdkafka,
    } as KafkaJS.CommonConstructorConfig);

    const producerConfig: ProducerConstructorConfig = options.producer
      ? { kafkaJS: this.toProducerConfig(name, options.producer) }
      : undefined;
    const producer = kafka.producer(producerConfig);

    this.connections.set(name, {
      kafka,
      producer,
      producerConfig,
      options,
      isProducerConnected: false,
      connectingPromise: null,
    });

    this.logger.log(
      `Kafka connection "${name}" registered (clientId: ${options.clientId})`,
    );
  }

  /**
   * Get Kafka instance by connection name
   */
  getKafka(name?: string): Kafka {
    const connectionName = name || DEFAULT_KAFKA_CONNECTION;
    const connection = this.connections.get(connectionName);

    if (!connection) {
      throw new Error(`Kafka connection "${connectionName}" not found`);
    }

    return connection.kafka;
  }

  /**
   * Get Producer by connection name
   */
  getProducer(name?: string): Producer {
    const connectionName = name || DEFAULT_KAFKA_CONNECTION;
    const connection = this.connections.get(connectionName);

    if (!connection) {
      throw new Error(`Kafka connection "${connectionName}" not found`);
    }

    return connection.producer;
  }

  /**
   * Connect producer for a specific connection
   */
  async connectProducer(name?: string): Promise<void> {
    const connectionName = name || DEFAULT_KAFKA_CONNECTION;
    const connection = this.connections.get(connectionName);

    if (!connection) {
      throw new Error(`Kafka connection "${connectionName}" not found`);
    }

    if (connection.isProducerConnected) return;

    // Guard against concurrent connect calls: reuse the in-flight promise
    if (connection.connectingPromise) {
      await connection.connectingPromise;
      return;
    }

    connection.connectingPromise = connection.producer
      .connect()
      .then(() => {
        connection.isProducerConnected = true;
        this.logger.log(`Producer connected for "${connectionName}"`);
      })
      .catch((error: unknown) => {
        // The client allows connect() once per producer: after a failure it
        // only answers "Connect has already been called elsewhere". Swap in a
        // fresh producer so the next send (e.g. a DLQ redelivery) reconnects.
        connection.producer = connection.kafka.producer(
          connection.producerConfig,
        );
        throw error;
      })
      .finally(() => {
        connection.connectingPromise = null;
      });

    await connection.connectingPromise;
  }

  /**
   * The connection's shared admin client (health probes), created and
   * connected on first use. Concurrent callers share the connect; a failed
   * connect is discarded so the next call builds a new admin. Rejects after
   * disconnectAll().
   */
  getAdmin(name?: string): Promise<Admin> {
    const connectionName = name || DEFAULT_KAFKA_CONNECTION;
    if (this.isShutDown) {
      return Promise.reject(new Error('Kafka connections are shut down'));
    }
    const connection = this.connections.get(connectionName);
    if (!connection) {
      return Promise.reject(
        new Error(`Kafka connection "${connectionName}" not found`),
      );
    }
    const cached = this.admins.get(connectionName);
    if (cached) return cached.ready;

    const admin = connection.kafka.admin();
    const ready = admin.connect().then(
      () => admin,
      (error: unknown) => {
        this.admins.delete(connectionName);
        throw error;
      },
    );
    this.admins.set(connectionName, { admin, ready });
    return ready;
  }

  /**
   * Check if a connection exists
   */
  hasConnection(name?: string): boolean {
    return this.connections.has(name || DEFAULT_KAFKA_CONNECTION);
  }

  /**
   * Check if the producer for a connection is actually connected
   */
  isProducerConnected(name?: string): boolean {
    return (
      this.connections.get(name || DEFAULT_KAFKA_CONNECTION)
        ?.isProducerConnected ?? false
    );
  }

  /**
   * Get all connection names
   */
  getConnectionNames(): string[] {
    return Array.from(this.connections.keys());
  }

  /**
   * Connect every producer at startup so health reflects real state. Not
   * awaited: an unreachable broker logs a warning and never blocks startup;
   * send() still connects lazily.
   */
  onApplicationBootstrap(): void {
    for (const name of this.connections.keys()) {
      this.connectProducerInBackground(name, 'at startup');
    }
  }

  /**
   * Start connecting `name`'s producer without awaiting it. No-op when it is
   * connected, a connect is in flight, or shutdown has begun. The health
   * indicator calls this for a disconnected producer, so a producer whose
   * startup connect failed recovers without waiting for a send().
   */
  connectProducerInBackground(
    name: string,
    context = 'in the background',
  ): void {
    const connection = this.connections.get(name);
    if (
      !connection ||
      this.isShutDown ||
      connection.isProducerConnected ||
      connection.connectingPromise
    ) {
      return;
    }
    this.connectProducer(name).catch((error) =>
      this.logger.warn(
        `Producer for connection "${name}" could not connect ${context}: ${String(error)}`,
      ),
    );
  }

  /**
   * Map an `ssl` object to librdkafka keys (the client only accepts a boolean
   * `ssl` in its kafkaJS block). Unknown options throw at registration.
   */
  private toSslKeys(
    name: string,
    ssl: SslOptions,
  ): Record<string, string | boolean> {
    const pem = (value: string | Buffer) =>
      Buffer.isBuffer(value) ? value.toString('utf-8') : value;
    const keys: Record<string, string | boolean> = {};

    for (const [option, value] of Object.entries(ssl)) {
      if (value === undefined) continue;
      switch (option as keyof SslOptions) {
        case 'ca':
          keys['ssl.ca.pem'] = (
            Array.isArray(value) ? value : [value as string | Buffer]
          )
            .map(pem)
            .join('\n');
          break;
        case 'cert':
          keys['ssl.certificate.pem'] = pem(value as string | Buffer);
          break;
        case 'key':
          keys['ssl.key.pem'] = pem(value as string | Buffer);
          break;
        case 'passphrase':
          keys['ssl.key.password'] = value as string;
          break;
        case 'rejectUnauthorized':
          if (value === false) {
            keys['enable.ssl.certificate.verification'] = false;
          }
          break;
        default:
          throw new Error(
            `Kafka connection "${name}": unsupported ssl option "${option}"; pass librdkafka keys through rdkafka`,
          );
      }
    }
    return keys;
  }

  setConsumerStatus(groupKey: string, status: ConsumerGroupStatus): void {
    this.consumerStatuses.set(groupKey, status);
  }

  getConsumerStatuses(): Record<string, ConsumerGroupStatus> {
    return Object.fromEntries(this.consumerStatuses);
  }

  /**
   * Disconnect all producers
   */
  async disconnectAll(): Promise<void> {
    this.isShutDown = true;
    // In parallel: the in-flight connect wait is bounded once, not per connection
    await Promise.all([
      ...[...this.connections].map(([name, connection]) =>
        this.disconnectProducer(name, connection),
      ),
      ...[...this.admins].map(([name, { admin, ready }]) =>
        this.disconnectAdmin(name, admin, ready),
      ),
    ]);
    this.admins.clear();
  }

  private async disconnectAdmin(
    name: string,
    admin: Admin,
    ready: Promise<Admin>,
  ): Promise<void> {
    try {
      await this.settleWithin(ready, IN_FLIGHT_CONNECT_WAIT_MS);
      await admin.disconnect();
    } catch (error) {
      this.logger.error(`Error disconnecting admin for "${name}"`, error);
    }
  }

  private async disconnectProducer(
    name: string,
    connection: KafkaConnection,
  ): Promise<void> {
    try {
      // A connect still in flight (e.g. the one started at bootstrap) must be
      // torn down too, or it finishes after shutdown and keeps the process
      // alive. Let it finish if it can, but never wait on it unbounded:
      // against an unreachable broker it hangs.
      if (connection.connectingPromise) {
        await this.settleWithin(
          connection.connectingPromise,
          IN_FLIGHT_CONNECT_WAIT_MS,
        );
      }
      if (connection.isProducerConnected || connection.connectingPromise) {
        await connection.producer.disconnect();
        connection.isProducerConnected = false;
        this.logger.log(`Producer disconnected for "${name}"`);
      }
    } catch (error) {
      this.logger.error(`Error disconnecting producer for "${name}"`, error);
    }
  }

  /** Resolves when `promise` settles or after `ms`, whichever comes first. */
  private async settleWithin(promise: Promise<unknown>, ms: number) {
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([
      promise.catch(() => undefined),
      new Promise((resolve) => {
        timer = setTimeout(resolve, ms);
      }),
    ]);
    clearTimeout(timer);
  }

  /**
   * KafkaJS-style producer keys must sit in the client's `kafkaJS` block, and
   * the client only accepts codec names for `compression`.
   */
  private toProducerConfig(
    connectionName: string,
    producer: ProducerConfig,
  ): KafkaJS.ProducerConfig {
    // An explicit `undefined` (e.g. an unset env var in forRootAsync) must not
    // reach the client: it checks key presence and hands the value to librdkafka.
    const rest = Object.fromEntries(
      Object.entries(producer).filter(
        ([key, value]) => key !== 'compression' && value !== undefined,
      ),
    );
    const { compression } = producer;
    if (compression === undefined) {
      return rest as KafkaJS.ProducerConfig;
    }
    const codec =
      typeof compression === 'number'
        ? COMPRESSION_CODECS[compression]
        : COMPRESSION_CODECS.find((c) => c === compression);
    if (!codec) {
      throw new Error(
        `Kafka connection "${connectionName}": unsupported compression ${typeof compression === 'string' ? `"${compression}"` : String(compression)}`,
      );
    }
    return {
      ...rest,
      compression: codec as KafkaJS.CompressionTypes,
    } as KafkaJS.ProducerConfig;
  }

  private mapLogLevel(level?: string): KafkaJS.logLevel {
    switch (level) {
      case 'NOTHING':
        return KafkaJS.logLevel.NOTHING;
      case 'ERROR':
        return KafkaJS.logLevel.ERROR;
      case 'WARN':
        return KafkaJS.logLevel.WARN;
      case 'INFO':
        return KafkaJS.logLevel.INFO;
      case 'DEBUG':
        return KafkaJS.logLevel.DEBUG;
      default:
        return KafkaJS.logLevel.INFO;
    }
  }
}
