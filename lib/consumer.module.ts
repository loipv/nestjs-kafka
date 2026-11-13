import {
  Module,
  DynamicModule,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Global,
  Provider,
  Inject,
  Optional,
} from '@nestjs/common';
import { DiscoveryModule, DiscoveryService } from '@nestjs/core';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { DlqService } from './services/dlq.service';
import { ConsumerModuleOptions, CONSUMER_MODULE_OPTIONS } from './interfaces';

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
    @Optional()
    @Inject(CONSUMER_MODULE_OPTIONS)
    private readonly options?: ConsumerModuleOptions,
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

    // Auto-discover @Consumer() methods on every provider (resolved in its
    // own module scope with its dependencies already injected).
    // Only static providers have a real instance at bootstrap.
    const wrappers = this.nestDiscoveryService.getProviders();
    this.discoveryService.warnSkipped(
      wrappers
        .filter((wrapper) => !wrapper.isDependencyTreeStatic())
        .map((wrapper): unknown => wrapper.metatype),
    );
    const consumers = this.discoveryService.discoverFromProviders(
      wrappers
        .filter((wrapper) => wrapper.isDependencyTreeStatic())
        .map((wrapper): unknown => wrapper.instance),
    );

    if (consumers.length === 0) return;
    try {
      this.registryService.registerConsumers(consumers);
    } catch (error) {
      await this.registryService.abortStartup();
      throw error;
    }
    // startAll() closes what it started before rejecting
    if (this.options?.autoStart !== false) {
      await this.registryService.startAll();
    }
  }

  // Only resets the start guard. ConsumerRegistryService disconnects the
  // consumers in its own onApplicationShutdown; nothing is restarted here.
  onApplicationShutdown(): void {
    this.isStarted = false;
  }
}
