import {
  DynamicModule,
  Global,
  Module,
  Provider,
  OnModuleInit,
  Inject,
  Optional,
} from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  KafkaOptionsFactory,
  KAFKA_MODULE_OPTIONS,
  DEFAULT_KAFKA_CONNECTION,
  getKafkaOptionsToken,
  getKafkaClientToken,
} from './interfaces';
import { KafkaCoreService } from './services/kafka-core.service';
import {
  KafkaClient,
  ConnectionBoundClient,
} from './services/kafka-client.service';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { PressureManagerService } from './services/pressure-manager.service';
import { DlqService } from './services/dlq.service';
import { KafkaHealthIndicator } from './health/kafka-health-indicator';
import { TerminusModule } from '@nestjs/terminus';

// Store registered connection names for tracking
const KAFKA_CONNECTION_NAMES = Symbol('KAFKA_CONNECTION_NAMES');

@Global()
@Module({})
export class KafkaModule implements OnModuleInit {
  private static isFirstModule = true;

  constructor(
    private readonly kafkaCore: KafkaCoreService,
    @Optional()
    @Inject(KAFKA_CONNECTION_NAMES)
    private readonly connectionNames?: string[],
  ) {}

  onModuleInit(): void {
    // Connection registration is handled by the factory providers
  }

  /**
   * Register a single Kafka connection
   */
  static forRoot(options: KafkaModuleOptions): DynamicModule {
    const connectionName = options.name || DEFAULT_KAFKA_CONNECTION;
    const optionsToken = getKafkaOptionsToken(connectionName);
    const clientToken = getKafkaClientToken(connectionName);

    const providers: Provider[] = [
      {
        provide: optionsToken,
        useValue: options,
      },
      {
        provide: `KAFKA_INIT_${connectionName}`,
        useFactory: (kafkaCore: KafkaCoreService) => {
          kafkaCore.registerConnection(options);
          return true;
        },
        inject: [KafkaCoreService],
      },
      // Named client provider for @InjectKafkaClient(connectionName)
      {
        provide: clientToken,
        useFactory: (
          kafkaClient: KafkaClient,
        ): KafkaClient | ConnectionBoundClient => {
          if (connectionName === DEFAULT_KAFKA_CONNECTION) {
            return kafkaClient;
          }
          return kafkaClient.forConnection(connectionName);
        },
        inject: [KafkaClient],
      },
    ];

    // Add core providers only on first module
    if (this.isFirstModule) {
      this.isFirstModule = false;
      providers.push(
        {
          provide: KAFKA_MODULE_OPTIONS,
          useValue: options,
        },
        KafkaCoreService,
        KafkaClient,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
        BatchProcessorService,
        IdempotencyService,
        PressureManagerService,
        DlqService,
        KafkaHealthIndicator,
      );
    }

    return {
      module: KafkaModule,
      imports: [DiscoveryModule, TerminusModule],
      providers,
      exports: [
        optionsToken,
        clientToken,
        KafkaCoreService,
        KafkaClient,
        KafkaHealthIndicator,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
      ],
    };
  }

  /**
   * Register multiple Kafka connections at once
   */
  static forRootMultiple(optionsArray: KafkaModuleOptions[]): DynamicModule {
    const providers: Provider[] = [];
    const exports: (string | symbol | Provider)[] = [
      KafkaCoreService,
      KafkaClient,
      KafkaHealthIndicator,
      ConsumerDiscoveryService,
      ConsumerRegistryService,
    ];

    // Create providers for each connection
    for (const options of optionsArray) {
      const connectionName = options.name || DEFAULT_KAFKA_CONNECTION;
      const optionsToken = getKafkaOptionsToken(connectionName);
      const clientToken = getKafkaClientToken(connectionName);

      providers.push(
        {
          provide: optionsToken,
          useValue: options,
        },
        {
          provide: `KAFKA_INIT_${connectionName}`,
          useFactory: (kafkaCore: KafkaCoreService) => {
            kafkaCore.registerConnection(options);
            return true;
          },
          inject: [KafkaCoreService],
        },
        // Named client provider for @InjectKafkaClient(connectionName)
        {
          provide: clientToken,
          useFactory: (
            kafkaClient: KafkaClient,
          ): KafkaClient | ConnectionBoundClient => {
            if (connectionName === DEFAULT_KAFKA_CONNECTION) {
              return kafkaClient;
            }
            return kafkaClient.forConnection(connectionName);
          },
          inject: [KafkaClient],
        },
      );

      exports.push(optionsToken);
      exports.push(clientToken);
    }

    // Use first connection as default
    const defaultOptions = optionsArray[0];
    providers.push(
      {
        provide: KAFKA_MODULE_OPTIONS,
        useValue: defaultOptions,
      },
      {
        provide: KAFKA_CONNECTION_NAMES,
        useValue: optionsArray.map((o) => o.name || DEFAULT_KAFKA_CONNECTION),
      },
      KafkaCoreService,
      KafkaClient,
      ConsumerDiscoveryService,
      ConsumerRegistryService,
      BatchProcessorService,
      IdempotencyService,
      PressureManagerService,
      DlqService,
      KafkaHealthIndicator,
    );

    this.isFirstModule = false;

    return {
      module: KafkaModule,
      imports: [DiscoveryModule, TerminusModule],
      providers,
      exports,
      global: true,
    };
  }

  /**
   * Register a Kafka connection asynchronously
   */
  static forRootAsync(options: KafkaModuleAsyncOptions): DynamicModule {
    const connectionName = options.name || DEFAULT_KAFKA_CONNECTION;
    const optionsToken = getKafkaOptionsToken(connectionName);
    const clientToken = getKafkaClientToken(connectionName);

    const asyncProviders = this.createAsyncProviders(options, optionsToken);

    const providers: Provider[] = [
      ...asyncProviders,
      {
        provide: `KAFKA_INIT_${connectionName}`,
        useFactory: (
          kafkaCore: KafkaCoreService,
          kafkaOptions: KafkaModuleOptions,
        ) => {
          kafkaCore.registerConnection({
            ...kafkaOptions,
            name: connectionName,
          });
          return true;
        },
        inject: [KafkaCoreService, optionsToken],
      },
      // Named client provider for @InjectKafkaClient(connectionName)
      {
        provide: clientToken,
        useFactory: (
          kafkaClient: KafkaClient,
        ): KafkaClient | ConnectionBoundClient => {
          if (connectionName === DEFAULT_KAFKA_CONNECTION) {
            return kafkaClient;
          }
          return kafkaClient.forConnection(connectionName);
        },
        inject: [KafkaClient],
      },
    ];

    // Add core providers only on first module
    if (this.isFirstModule) {
      this.isFirstModule = false;
      providers.push(
        KafkaCoreService,
        KafkaClient,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
        BatchProcessorService,
        IdempotencyService,
        PressureManagerService,
        DlqService,
        KafkaHealthIndicator,
      );
    }

    return {
      module: KafkaModule,
      imports: [...(options.imports || []), DiscoveryModule, TerminusModule],
      providers,
      exports: [
        optionsToken,
        clientToken,
        KafkaCoreService,
        KafkaClient,
        KafkaHealthIndicator,
        ConsumerDiscoveryService,
        ConsumerRegistryService,
      ],
      global: options.global ?? true,
    };
  }

  private static createAsyncProviders(
    options: KafkaModuleAsyncOptions,
    optionsToken: string,
  ): Provider[] {
    if (options.useFactory) {
      return [
        {
          provide: optionsToken,
          useFactory: options.useFactory,
          inject: options.inject || [],
        },
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
        provide: optionsToken,
        useFactory: async (optionsFactory: KafkaOptionsFactory) =>
          await optionsFactory.createKafkaOptions(),
        inject: [useClass],
      },
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
