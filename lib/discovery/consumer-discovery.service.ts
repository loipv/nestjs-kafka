import { Injectable, OnModuleInit, Logger } from '@nestjs/common';
import { DiscoveryService, MetadataScanner, Reflector } from '@nestjs/core';
import { KAFKA_CONSUMER_METADATA } from '../decorators/constants';
import { ConsumerMethodMetadata } from '../decorators/consumer.decorator';
import { ConsumerMetadata } from '../interfaces';

@Injectable()
export class ConsumerDiscoveryService implements OnModuleInit {
  private readonly logger = new Logger(ConsumerDiscoveryService.name);
  private discoveredConsumers: ConsumerMetadata[] = [];

  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly reflector: Reflector,
    private readonly metadataScanner: MetadataScanner,
  ) {}

  onModuleInit(): void {
    this.discoverConsumers();
  }

  discoverConsumers(): ConsumerMetadata[] {
    if (this.discoveredConsumers.length > 0) {
      return this.discoveredConsumers;
    }

    const providers = this.discoveryService.getProviders();

    for (const wrapper of providers) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const { instance, metatype } = wrapper;

      if (!instance || !metatype) {
        continue;
      }

      const prototype = Object.getPrototypeOf(instance) as object;
      const methodNames = this.metadataScanner.getAllMethodNames(prototype);

      for (const methodName of methodNames) {
        const methodRef = prototype[methodName as keyof typeof prototype];

        if (typeof methodRef !== 'function') {
          continue;
        }

        const metadata = this.reflector.get<ConsumerMethodMetadata>(
          KAFKA_CONSUMER_METADATA,
          methodRef,
        );

        if (!metadata) {
          continue;
        }

        // Skip disabled consumers
        if (metadata.options.disabled) {
          this.logger.log(
            `Skipping disabled consumer: ${metatype.name}.${methodName} for topic: ${metadata.topic}`,
          );
          continue;
        }

        const consumerMetadata: ConsumerMetadata = {
          topic: metadata.topic,
          options: {
            ...metadata.options,
            topic: metadata.topic,
          },
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
          target: instance,
          methodName,
        };

        this.discoveredConsumers.push(consumerMetadata);
        this.logger.log(
          `Discovered consumer: ${metatype.name}.${methodName} for topic: ${metadata.topic}`,
        );
      }
    }

    return this.discoveredConsumers;
  }

  getConsumers(): ConsumerMetadata[] {
    return this.discoveredConsumers;
  }
}
