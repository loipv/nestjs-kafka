import { Injectable, Logger } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaModuleOptions, DEFAULT_KAFKA_CONNECTION } from '../interfaces';

type Kafka = InstanceType<typeof KafkaJS.Kafka>;
type Producer = KafkaJS.Producer;

interface KafkaConnection {
  kafka: Kafka;
  producer: Producer;
  options: KafkaModuleOptions;
  isProducerConnected: boolean;
  connectingPromise: Promise<void> | null;
}

@Injectable()
export class KafkaCoreService {
  private readonly logger = new Logger(KafkaCoreService.name);
  private connections = new Map<string, KafkaConnection>();

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
    if (!brokers || (Array.isArray(brokers) && brokers.length === 0)) {
      throw new Error(
        `Kafka connection "${name}": brokers is required and cannot be empty`,
      );
    }

    if (this.connections.has(name)) {
      this.logger.warn(
        `Kafka connection "${name}" already registered, skipping`,
      );
      return;
    }

    // Build Kafka config with only defined values to avoid undefined property issues
    const kafkaConfig: KafkaJS.KafkaConfig = {
      clientId: options.clientId,
      brokers: options.brokers as string[],
      logLevel: this.mapLogLevel(options.logLevel),
      // Use default library logger - custom logger can cause issues with internal log formatting
    };

    // Add optional settings only if defined
    if (options.ssl !== undefined) {
      kafkaConfig.ssl =
        typeof options.ssl === 'boolean' ? options.ssl : !!options.ssl;
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

    const kafka = new KafkaJS.Kafka({ kafkaJS: kafkaConfig });

    const producer = kafka.producer(options.producer);

    this.connections.set(name, {
      kafka,
      producer,
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
      .finally(() => {
        connection.connectingPromise = null;
      });

    await connection.connectingPromise;
  }

  /**
   * Get options for a specific connection
   */
  getOptions(name?: string): KafkaModuleOptions {
    const connectionName = name || DEFAULT_KAFKA_CONNECTION;
    const connection = this.connections.get(connectionName);

    if (!connection) {
      throw new Error(`Kafka connection "${connectionName}" not found`);
    }

    return connection.options;
  }

  /**
   * Check if a connection exists
   */
  hasConnection(name?: string): boolean {
    return this.connections.has(name || DEFAULT_KAFKA_CONNECTION);
  }

  /**
   * Get all connection names
   */
  getConnectionNames(): string[] {
    return Array.from(this.connections.keys());
  }

  /**
   * Disconnect all producers
   */
  async disconnectAll(): Promise<void> {
    for (const [name, connection] of this.connections) {
      try {
        if (connection.isProducerConnected) {
          await connection.producer.disconnect();
          connection.isProducerConnected = false;
          this.logger.log(`Producer disconnected for "${name}"`);
        }
      } catch (error) {
        this.logger.error(`Error disconnecting producer for "${name}"`, error);
      }
    }
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
