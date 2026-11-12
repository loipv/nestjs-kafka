/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
import {
  Module,
  DynamicModule,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Global,
  Provider,
} from '@nestjs/common';
import { DiscoveryModule, DiscoveryService } from '@nestjs/core';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { DlqService } from './services/dlq.service';
import { ConsumerModuleOptions, CONSUMER_MODULE_OPTIONS } from './interfaces';
import { KAFKA_CONSUMER_METADATA } from './decorators/constants';

// All consumer-related providers (moved from KafkaModule)
const CONSUMER_PROVIDERS: Provider[] = [
  ConsumerDiscoveryService,
  ConsumerRegistryService,
  BatchProcessorService,
  IdempotencyService,
  DlqService,
];

/**
 * ConsumerModule handles automatic discovery and registration of Kafka consumers.
 *
 * All providers with methods decorated with `@Consumer()` are automatically
 * discovered using NestJS DiscoveryService at application startup. No explicit
 * registration is needed — just declare your consumer class as a provider
 * in any module.
 *
 * @example
 * // app.module.ts
 * @Module({
 *   imports: [
 *     KafkaModule.forRoot({ ... }),
 *     ConsumerModule.forRoot(),
 *     OrderModule,
 *   ],
 * })
 * export class AppModule {}
 *
 * // order/order.module.ts
 * @Module({
 *   providers: [OrderConsumer, OrderService],
 * })
 * export class OrderModule {}
 *
 * // order/order.consumer.ts
 * @Injectable()
 * export class OrderConsumer {
 *   constructor(private readonly orderService: OrderService) {}
 *
 *   @Consumer('orders')
 *   async handleOrder(message: KafkaMessage) { ... }
 * }
 */
@Global()
@Module({})
export class ConsumerModule
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private isStarted = false;

  constructor(
    private readonly discoveryService: ConsumerDiscoveryService,
    private readonly registryService: ConsumerRegistryService,
    private readonly nestDiscoveryService: DiscoveryService,
  ) {}

  /**
   * Register the core ConsumerModule. Call once in root module.
   * This module depends on KafkaModule being imported first.
   *
   * All providers with `@Consumer()` decorated methods across the entire
   * application are automatically discovered — no additional registration needed.
   *
   * @param options - Default options for all consumers (decorator options take precedence)
   * @example
   * ConsumerModule.forRoot({
   *   partitionAssigners: ['cooperative-sticky'],
   *   allowAutoTopicCreation: true,
   *   sessionTimeout: 30000,
   * })
   */
  static forRoot(options?: ConsumerModuleOptions): DynamicModule {
    return {
      module: ConsumerModule,
      global: true,
      imports: [DiscoveryModule],
      providers: [
        {
          provide: CONSUMER_MODULE_OPTIONS,
          useValue: options || {},
        },
        ...CONSUMER_PROVIDERS,
      ],
      exports: [
        CONSUMER_MODULE_OPTIONS,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
        DlqService,
      ],
    };
  }

  /**
   * Consumers start after every provider's onModuleInit (database pools,
   * caches...), so the first message never reaches a half-initialised app.
   */
  async onApplicationBootstrap(): Promise<void> {
    // Only start once per instance
    if (this.isStarted) {
      return;
    }
    this.isStarted = true;

    // Auto-discover all providers with @Consumer() decorated methods
    // using NestJS DiscoveryService. Consumers are resolved in their own
    // module scope with all their dependencies already injected.
    const consumerInstances = this.discoverConsumers();

    if (consumerInstances.length > 0) {
      this.discoveryService.discoverFromProviders(consumerInstances);
    }

    const consumers = this.discoveryService.getConsumers();

    if (consumers.length > 0) {
      try {
        this.registryService.registerConsumers(consumers);
        await this.registryService.startAll();
      } catch (error) {
        await this.registryService.abortStartup();
        throw error;
      }
    }
  }

  /**
   * Discover all providers in the application that have methods
   * decorated with @Consumer(). Uses NestJS DiscoveryService to scan
   * all registered providers across all modules.
   */
  private discoverConsumers(): any[] {
    const consumerInstances: any[] = [];
    const providers = this.nestDiscoveryService.getProviders();

    for (const wrapper of providers) {
      const instance = wrapper.instance;
      if (!instance || typeof instance !== 'object') continue;

      // Check if any method has @Consumer metadata
      const prototype = Object.getPrototypeOf(instance);
      if (!prototype) continue;

      try {
        const methodNames = Object.getOwnPropertyNames(prototype).filter(
          (name) => {
            if (name === 'constructor') return false;
            // Use property descriptor to safely check if it's a method
            // without triggering getter side effects (e.g. HttpAdapterHost.listen$)
            const descriptor = Object.getOwnPropertyDescriptor(prototype, name);
            return (
              descriptor &&
              typeof descriptor.value === 'function' &&
              !descriptor.get
            );
          },
        );

        const hasConsumerDecorator = methodNames.some((methodName) => {
          const metadata = Reflect.getMetadata(
            KAFKA_CONSUMER_METADATA,
            prototype[methodName],
          );
          return !!metadata;
        });

        if (hasConsumerDecorator) {
          consumerInstances.push(instance);
        }
      } catch {
        // Skip providers that throw errors when inspecting their prototype
        continue;
      }
    }

    return consumerInstances;
  }

  // ConsumerRegistryService shuts the consumers down in its own
  // onApplicationShutdown; this only allows a restarted app to start again.
  onApplicationShutdown(): void {
    this.isStarted = false;
  }
}
