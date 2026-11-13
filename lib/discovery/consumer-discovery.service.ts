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
   * Discover @Consumer methods on provider instances (one pass). Each
   * instance is scanned once, even when Nest lists it under several tokens
   * (`useExisting` aliases). Skips non-objects and prototype-less objects.
   */
  discoverFromProviders(instances: unknown[]): ConsumerMetadata[] {
    const seen = new Set<object>();
    for (const instance of instances) {
      if (!instance || typeof instance !== 'object' || seen.has(instance)) {
        continue;
      }
      seen.add(instance);
      const prototype = Object.getPrototypeOf(instance) as object | null;
      if (!prototype) continue;

      const className = this.className(prototype);
      for (const { methodName, metadata } of this.consumerMethods(prototype)) {
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

  /**
   * @Consumer methods along the prototype chain, up to (excluding)
   * Object.prototype. Each name counts once, at the nearest class, so an
   * override without @Consumer hides a decorated base method. Only data
   * descriptors are read (no getters, no proxy traps); a level that throws on
   * inspection ends the walk.
   */
  consumerMethods(
    prototype: object,
  ): Array<{ methodName: string; metadata: ConsumerMethodMetadata }> {
    const found: Array<{
      methodName: string;
      metadata: ConsumerMethodMetadata;
    }> = [];
    const visited = new Set<string>(['constructor']);
    let level: object | null = prototype;
    while (level && level !== Object.prototype) {
      try {
        for (const name of Object.getOwnPropertyNames(level)) {
          if (visited.has(name)) continue;
          visited.add(name);
          const descriptor = Object.getOwnPropertyDescriptor(level, name);
          if (
            !descriptor ||
            descriptor.get ||
            typeof descriptor.value !== 'function'
          ) {
            continue;
          }
          const metadata: ConsumerMethodMetadata | undefined =
            Reflect.getMetadata(
              KAFKA_CONSUMER_METADATA,
              descriptor.value as object,
            );
          if (metadata) found.push({ methodName: name, metadata });
        }
        level = Object.getPrototypeOf(level) as object | null;
      } catch {
        break;
      }
    }
    return found;
  }

  /**
   * Warn about @Consumer methods on non-static providers (request-scoped, or
   * depending on one): Nest has no real instance for them at bootstrap, so
   * they are not registered.
   */
  warnSkipped(metatypes: unknown[]): void {
    for (const metatype of new Set(metatypes)) {
      if (typeof metatype !== 'function') continue;
      const prototype = (metatype as { prototype?: unknown }).prototype;
      if (!prototype || typeof prototype !== 'object') continue;
      const className = this.className(prototype);
      for (const { methodName } of this.consumerMethods(prototype)) {
        this.logger.warn(
          `Skipping consumer ${className}.${methodName}: request-scoped providers cannot consume`,
        );
      }
    }
  }

  /** Class name from the prototype's own `constructor` descriptor (no reads). */
  private className(prototype: object): string | undefined {
    try {
      return (
        Object.getOwnPropertyDescriptor(prototype, 'constructor')?.value as
          | { name?: string }
          | undefined
      )?.name;
    } catch {
      return undefined;
    }
  }

  getConsumers(): ConsumerMetadata[] {
    return this.discoveredConsumers;
  }
}
