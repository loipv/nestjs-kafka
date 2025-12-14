import { DynamicModule, Global, Module, Provider } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  KafkaOptionsFactory,
  KAFKA_MODULE_OPTIONS,
} from './interfaces';
import { KafkaCoreService } from './services/kafka-core.service';
import { KafkaClient } from './services/kafka-client.service';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { PressureManagerService } from './services/pressure-manager.service';
import { DlqService } from './services/dlq.service';
import { KafkaHealthIndicator } from './health/kafka-health-indicator';
import { TerminusModule } from '@nestjs/terminus';

@Global()
@Module({})
export class KafkaModule {
  static forRoot(options: KafkaModuleOptions): DynamicModule {
    const optionsProvider: Provider = {
      provide: KAFKA_MODULE_OPTIONS,
      useValue: options,
    };

    return {
      module: KafkaModule,
      imports: [DiscoveryModule, TerminusModule],
      providers: [
        optionsProvider,
        KafkaCoreService,
        KafkaClient,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
        BatchProcessorService,
        IdempotencyService,
        PressureManagerService,
        DlqService,
        KafkaHealthIndicator,
      ],
      exports: [
        KafkaClient,
        KafkaHealthIndicator,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
        KAFKA_MODULE_OPTIONS,
      ],
    };
  }

  static forRootAsync(options: KafkaModuleAsyncOptions): DynamicModule {
    const asyncProviders = this.createAsyncProviders(options);

    return {
      module: KafkaModule,
      imports: [...(options.imports || []), DiscoveryModule],
      providers: [
        ...asyncProviders,
        KafkaCoreService,
        KafkaClient,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
        BatchProcessorService,
        IdempotencyService,
        PressureManagerService,
        DlqService,
        KafkaHealthIndicator,
      ],
      exports: [
        KafkaClient,
        KafkaHealthIndicator,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
        KAFKA_MODULE_OPTIONS,
      ],
      global: options.global ?? true,
    };
  }

  private static createAsyncProviders(
    options: KafkaModuleAsyncOptions,
  ): Provider[] {
    if (options.useFactory) {
      return [
        {
          provide: KAFKA_MODULE_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject || [],
        },
      ];
    }

    const useClass = options.useClass || options.useExisting;
    if (!useClass) {
      throw new Error('Invalid KafkaModuleAsyncOptions');
    }

    return [
      {
        provide: KAFKA_MODULE_OPTIONS,
        useFactory: async (optionsFactory: KafkaOptionsFactory) =>
          await optionsFactory.createKafkaOptions(),
        inject: [useClass],
      },
      ...(options.useClass ? [{ provide: useClass, useClass }] : []),
    ];
  }
}
