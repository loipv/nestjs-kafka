import { Injectable, Inject, OnModuleInit, Logger } from '@nestjs/common';
import { Kafka, logLevel } from 'kafkajs';
import { KafkaModuleOptions, KAFKA_MODULE_OPTIONS } from '../interfaces';

@Injectable()
export class KafkaCoreService implements OnModuleInit {
  private readonly logger = new Logger(KafkaCoreService.name);
  private kafka: Kafka;

  constructor(
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
  ) {}

  onModuleInit(): void {
    this.kafka = new Kafka({
      clientId: this.options.clientId,
      brokers: this.options.brokers as string[],
      ssl: this.options.ssl,
      sasl: this.options.sasl,
      connectionTimeout: this.options.connectionTimeout,
      requestTimeout: this.options.requestTimeout,
      enforceRequestTimeout: this.options.enforceRequestTimeout,
      retry: this.options.retry,
      logLevel: this.mapLogLevel(this.options.logLevel),
      logCreator:
        () =>
        ({ level, log }) => {
          const { message, ...extra } = log;
          switch (level) {
            case logLevel.ERROR:
              this.logger.error(message, extra);
              break;
            case logLevel.WARN:
              this.logger.warn(message, extra);
              break;
            case logLevel.INFO:
              this.logger.log(message);
              break;
            case logLevel.DEBUG:
              this.logger.debug(message);
              break;
          }
        },
    });

    this.logger.log(`Kafka client initialized: ${this.options.clientId}`);
  }

  getKafka(): Kafka {
    return this.kafka;
  }

  getOptions(): KafkaModuleOptions {
    return this.options;
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
