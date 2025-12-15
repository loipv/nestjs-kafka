import { Injectable, Logger } from '@nestjs/common';
import { Kafka, logLevel, Producer } from 'kafkajs';
import { KafkaModuleOptions, DEFAULT_KAFKA_CONNECTION } from '../interfaces';

interface KafkaConnection {
  kafka: Kafka;
  producer: Producer;
  options: KafkaModuleOptions;
  isProducerConnected: boolean;
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

    if (this.connections.has(name)) {
      this.logger.warn(
        `Kafka connection "${name}" already registered, skipping`,
      );
      return;
    }

    const kafka = new Kafka({
      clientId: options.clientId,
      brokers: options.brokers as string[],
      ssl: options.ssl,
      sasl: options.sasl,
      connectionTimeout: options.connectionTimeout,
      requestTimeout: options.requestTimeout,
      enforceRequestTimeout: options.enforceRequestTimeout,
      retry: options.retry,
      logLevel: this.mapLogLevel(options.logLevel),
      logCreator:
        () =>
        ({ level, log }) => {
          const { message, ...extra } = log;
          switch (level) {
            case logLevel.ERROR:
              this.logger.error(`[${name}] ${message}`, extra);
              break;
            case logLevel.WARN:
              this.logger.warn(`[${name}] ${message}`, extra);
              break;
            case logLevel.INFO:
              this.logger.log(`[${name}] ${message}`);
              break;
            case logLevel.DEBUG:
              this.logger.debug(`[${name}] ${message}`);
              break;
          }
        },
    });

    const producer = kafka.producer(options.producer);

    this.connections.set(name, {
      kafka,
      producer,
      options,
      isProducerConnected: false,
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

    if (!connection.isProducerConnected) {
      await connection.producer.connect();
      connection.isProducerConnected = true;
      this.logger.log(`Producer connected for "${connectionName}"`);
    }
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

  private mapLogLevel(level?: string): logLevel {
    switch (level) {
      case 'NOTHING':
        return logLevel.NOTHING;
      case 'ERROR':
        return logLevel.ERROR;
      case 'WARN':
        return logLevel.WARN;
      case 'INFO':
        return logLevel.INFO;
      case 'DEBUG':
        return logLevel.DEBUG;
      default:
        return logLevel.INFO;
    }
  }
}
