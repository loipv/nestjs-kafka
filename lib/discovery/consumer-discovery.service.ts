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

      // [name, function] pairs from data descriptors only: never a property
      // read through the prototype, so no getter or proxy trap runs.
      let methods: Array<[string, object]>;
      let className: string | undefined;
      try {
        className = (
          Object.getOwnPropertyDescriptor(prototype, 'constructor')?.value as
            | { name?: string }
            | undefined
        )?.name;
        methods = Object.getOwnPropertyNames(prototype).flatMap(
          (name): Array<[string, object]> => {
            if (name === 'constructor') return [];
            const descriptor = Object.getOwnPropertyDescriptor(prototype, name);
            return descriptor &&
              !descriptor.get &&
              typeof descriptor.value === 'function'
              ? [[name, descriptor.value as object]]
              : [];
          },
        );
      } catch {
        continue;
      }

      for (const [methodName, method] of methods) {
        const metadata: ConsumerMethodMetadata | undefined =
          Reflect.getMetadata(KAFKA_CONSUMER_METADATA, method);
        if (!metadata) continue;

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
