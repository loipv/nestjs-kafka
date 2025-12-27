import {
  DynamicModule,
  Global,
  Module,
  Provider,
  OnModuleInit,
  Inject,
  Optional,
} from '@nestjs/common';
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
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { PressureManagerService } from './services/pressure-manager.service';
import { DlqService } from './services/dlq.service';
import { DlqRetryService } from './services/dlq-retry.service';
import { KafkaHealthIndicator } from './health/kafka-health-indicator';

// Store registered connection names for tracking
const KAFKA_CONNECTION_NAMES = Symbol('KAFKA_CONNECTION_NAMES');

// Core providers that should be singleton across the app
// KafkaHealthIndicator is included but requires TerminusModule to be imported by the user
const CORE_PROVIDERS: Provider[] = [
  KafkaCoreService,
  KafkaClient,
  ConsumerRegistryService,
  BatchProcessorService,
  IdempotencyService,
  PressureManagerService,
  DlqService,
  DlqRetryService,
  KafkaHealthIndicator,
];

@Global()
@Module({})
export class KafkaModule implements OnModuleInit {
  constructor(
    private readonly kafkaCore: KafkaCoreService,
    @Optional()
    @Inject(KAFKA_CONNECTION_NAMES)
    private readonly connectionNames?: string[],
  ) { }

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
        provide: KAFKA_MODULE_OPTIONS,
        useValue: options,
      },
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
      ...CORE_PROVIDERS,
    ];

    return {
      module: KafkaModule,
      providers,
      exports: [
        optionsToken,
        clientToken,
        KafkaCoreService,
        KafkaClient,
        KafkaHealthIndicator,
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
      ...CORE_PROVIDERS,
    );

    return {
      module: KafkaModule,
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
      ...CORE_PROVIDERS,
    ];

    return {
      module: KafkaModule,
      imports: [...(options.imports || [])],
      providers,
      exports: [
        optionsToken,
        clientToken,
        KafkaCoreService,
        KafkaClient,
        KafkaHealthIndicator,
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
