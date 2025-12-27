import { Injectable, Logger } from '@nestjs/common';
import { KAFKA_CONSUMER_METADATA } from '../decorators/constants';
import { ConsumerMethodMetadata } from '../decorators/consumer.decorator';
import { ConsumerMetadata, DEFAULT_KAFKA_CONNECTION } from '../interfaces';

@Injectable()
export class ConsumerDiscoveryService {
  private readonly logger = new Logger(ConsumerDiscoveryService.name);
  private discoveredConsumers: ConsumerMetadata[] = [];

  /**
   * Manually register a consumer handler.
   * Use this when auto-discovery doesn't work.
   */
  registerHandler(
    target: any,
    methodName: string,
    topic: string,
    options: Partial<ConsumerMetadata['options']> = {},
  ): void {
    const connection = options.connection || DEFAULT_KAFKA_CONNECTION;

    const consumerMetadata: ConsumerMetadata = {
      topic,
      connection,
      options: {
        ...options,
        topic,
        connection,
      } as ConsumerMetadata['options'],
      target,
      methodName,
    };

    this.discoveredConsumers.push(consumerMetadata);
    this.logger.log(
      `Registered consumer handler: ${target.constructor?.name || 'Unknown'}.${methodName} for topic: ${topic}`,
    );
  }

  /**
   * Discover consumers from an array of provider instances.
   * Call this with your consumer service instances.
   */
  discoverFromProviders(providers: any[]): ConsumerMetadata[] {
    for (const instance of providers) {
      if (!instance) continue;

      const prototype = Object.getPrototypeOf(instance);
      const methodNames = Object.getOwnPropertyNames(prototype).filter(
        (name) => name !== 'constructor' && typeof prototype[name] === 'function',
      );

      for (const methodName of methodNames) {
        const methodRef = prototype[methodName];

        // Check for @Consumer decorator metadata
        const metadata: ConsumerMethodMetadata | undefined = Reflect.getMetadata(
          KAFKA_CONSUMER_METADATA,
          methodRef,
        );

        if (!metadata) continue;

        // Skip disabled consumers
        if (metadata.options.disabled) {
          this.logger.log(
            `Skipping disabled consumer: ${instance.constructor?.name}.${methodName} for topic: ${metadata.topic}`,
          );
          continue;
        }

        const connection = metadata.options.connection || DEFAULT_KAFKA_CONNECTION;

        const consumerMetadata: ConsumerMetadata = {
          topic: metadata.topic,
          connection,
          options: {
            ...metadata.options,
            topic: metadata.topic,
            connection,
          },
          target: instance,
          methodName,
        };

        this.discoveredConsumers.push(consumerMetadata);
        this.logger.log(
          `Discovered consumer: ${instance.constructor?.name}.${methodName} for topic: ${metadata.topic} (connection: ${connection})`,
        );
      }
    }

    return this.discoveredConsumers;
  }

  getConsumers(): ConsumerMetadata[] {
    return this.discoveredConsumers;
  }

  clearConsumers(): void {
    this.discoveredConsumers = [];
  }
}
