import {
  Module,
  DynamicModule,
  OnModuleInit,
  OnApplicationShutdown,
  Type,
  Global,
  Provider,
} from '@nestjs/common';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { PressureManagerService } from './services/pressure-manager.service';
import { DlqService } from './services/dlq.service';
import { DlqRetryService } from './services/dlq-retry.service';
import { DlqMetricsService } from './services/dlq-metrics.service';
import { CircuitBreakerService } from './services/circuit-breaker.service';
import { ConsumerModuleOptions, CONSUMER_MODULE_OPTIONS } from './interfaces';

// All consumer-related providers (moved from KafkaModule)
const CONSUMER_PROVIDERS: Provider[] = [
  ConsumerDiscoveryService,
  ConsumerRegistryService,
  BatchProcessorService,
  IdempotencyService,
  PressureManagerService,
  DlqMetricsService,
  CircuitBreakerService,
  DlqService,
  DlqRetryService,
];

/**
 * ConsumerModule handles discovery and registration of Kafka consumers.
 *
 * @example
 * // app.module.ts
 * @Module({
 *   imports: [
 *     KafkaModule.forRoot({ ... }),
 *     ConsumerModule.forRoot(),
 *     TestModule,
 *   ],
 * })
 * export class AppModule {}
 *
 * // test/test.module.ts
 * @Module({
 *   imports: [ConsumerModule.forFeature([TestService])],
 *   providers: [TestService],
 * })
 * export class TestModule {}
 */
@Global()
@Module({})
export class ConsumerModule implements OnModuleInit, OnApplicationShutdown {
  private static consumerInstances: any[] = [];
  private static isStarted = false;

  constructor(
    private readonly discoveryService: ConsumerDiscoveryService,
    private readonly registryService: ConsumerRegistryService,
  ) {}

  /**
   * Register the core ConsumerModule. Call once in root module.
   * This module depends on KafkaModule being imported first.
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
        DlqRetryService,
        DlqMetricsService,
        CircuitBreakerService,
      ],
    };
  }

  /**
   * Register consumers from a feature module.
   * Consumer classes must also be in the feature module's providers array.
   *
   * @param consumers - Consumer classes with @Consumer decorated methods
   */
  static forFeature(consumers: Type<any>[]): DynamicModule {
    // Create unique provider that collects consumer instances
    const collectorProvider: Provider = {
      provide: `KAFKA_COLLECTOR_${Date.now()}_${Math.random().toString(36)}`,
      useFactory: (...instances: any[]): any[] => {
        // Add to static collection for later discovery
        ConsumerModule.consumerInstances.push(...instances);
        return instances;
      },
      inject: consumers,
    };

    return {
      module: ConsumerModule,
      // Include consumer classes as providers so they can be injected
      providers: [...consumers, collectorProvider],
      exports: consumers,
    };
  }

  static getConsumerInstances(): any[] {
    return ConsumerModule.consumerInstances;
  }

  static clearConsumers(): void {
    ConsumerModule.consumerInstances = [];
    ConsumerModule.isStarted = false;
  }

  async onModuleInit(): Promise<void> {
    // Only start once
    if (ConsumerModule.isStarted) {
      return;
    }
    ConsumerModule.isStarted = true;

    // Wait for all feature modules to collect their consumers
    // await new Promise((resolve) => setTimeout(resolve, 100));

    // Discover from all collected instances
    const allInstances = ConsumerModule.getConsumerInstances();
    if (allInstances.length > 0) {
      this.discoveryService.discoverFromProviders(allInstances);
    }

    const consumers = this.discoveryService.getConsumers();

    if (consumers.length > 0) {
      this.registryService.registerConsumers(consumers);
      await this.registryService.startAll();
    }
  }

  async onApplicationShutdown(): Promise<void> {
    await this.registryService.gracefulShutdown();
    ConsumerModule.clearConsumers();
  }
}
