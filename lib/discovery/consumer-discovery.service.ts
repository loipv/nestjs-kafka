/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { Injectable, Logger } from '@nestjs/common';
import { KAFKA_CONSUMER_METADATA } from '../decorators/constants';
import { ConsumerMethodMetadata } from '../decorators/consumer.decorator';
import { ConsumerMetadata, DEFAULT_KAFKA_CONNECTION } from '../interfaces';

@Injectable()
export class ConsumerDiscoveryService {
  private readonly logger = new Logger(ConsumerDiscoveryService.name);
  private discoveredConsumers: ConsumerMetadata[] = [];

  /**
   * Discover @Consumer methods on provider instances (one pass). Skips
   * non-objects, prototype-less objects and prototypes that throw on
   * inspection; only own data properties are read, never getters (some
   * providers, e.g. Nest's HttpAdapterHost, have getters with side effects).
   */
  discoverFromProviders(instances: unknown[]): ConsumerMetadata[] {
    for (const instance of instances) {
      if (!instance || typeof instance !== 'object') continue;
      const prototype = Object.getPrototypeOf(instance) as Record<
        string,
        unknown
      > | null;
      if (!prototype) continue;

      let methodNames: string[];
      try {
        methodNames = Object.getOwnPropertyNames(prototype).filter((name) => {
          if (name === 'constructor') return false;
          const descriptor = Object.getOwnPropertyDescriptor(prototype, name);
          return (
            !!descriptor &&
            !descriptor.get &&
            typeof descriptor.value === 'function'
          );
        });
      } catch {
        continue;
      }

      for (const methodName of methodNames) {
        const metadata: ConsumerMethodMetadata | undefined =
          Reflect.getMetadata(
            KAFKA_CONSUMER_METADATA,
            prototype[methodName] as object,
          );
        if (!metadata) continue;

        const className = instance.constructor?.name;

        // Skip disabled consumers
        if (metadata.options.disabled) {
          this.logger.log(
            `Skipping disabled consumer: ${className}.${methodName} for topic: ${metadata.topic}`,
          );
          continue;
        }

        const connection =
          metadata.options.connection || DEFAULT_KAFKA_CONNECTION;

        this.discoveredConsumers.push({
          topic: metadata.topic,
          connection,
          options: { ...metadata.options, connection },
          target: instance,
          methodName,
        });
        this.logger.log(
          `Discovered consumer: ${className}.${methodName} for topic: ${metadata.topic} (connection: ${connection})`,
        );
      }
    }

    return this.discoveredConsumers;
  }

  getConsumers(): ConsumerMetadata[] {
    return this.discoveredConsumers;
  }
}
