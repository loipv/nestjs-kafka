import {
  Module,
  DynamicModule,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Global,
  Provider,
  Inject,
  Optional,
  Scope,
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
   * Discover and register consumers, then start them unless
   * `autoStart: false`. Every provider's onModuleInit has run; other modules'
   * onApplicationBootstrap may not have (Nest bootstraps global modules first).
   */
  async onApplicationBootstrap(): Promise<void> {
    // Only start once per instance
    if (this.isStarted) {
      return;
    }
    this.isStarted = true;

    // Auto-discover @Consumer() methods on every provider (resolved in its
    // own module scope with its dependencies already injected).
    // Only singletons have a real instance at bootstrap: request-scoped trees
    // have none, and a transient wrapper's instance never ran its constructor.
    const wrappers = this.nestDiscoveryService.getProviders();
    const isSingleton = (wrapper: (typeof wrappers)[number]) =>
      wrapper.isDependencyTreeStatic() && !wrapper.isTransient;
    this.discoveryService.warnSkipped(
      wrappers
        .filter((wrapper) => !isSingleton(wrapper))
        .map((wrapper) => ({
          metatype: wrapper.metatype as unknown,
          reason: wrapper.isTransient
            ? 'transient providers'
            : wrapper.scope === Scope.REQUEST
              ? 'request-scoped providers'
              : 'providers that depend on a request-scoped provider',
        })),
    );
    const consumers = this.discoveryService.discoverFromProviders(
      wrappers.filter(isSingleton).map((wrapper): unknown => wrapper.instance),
    );

    // Always register (even nothing): startAll() refuses to run before it
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
  // consumers earlier, in its own onModuleDestroy; nothing is restarted here.
  onApplicationShutdown(): void {
    this.isStarted = false;
  }
}
