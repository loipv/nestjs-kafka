import {
  DynamicModule,
  Global,
  Module,
  Provider,
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
import { TracingService } from './services/tracing.service';

import { KafkaHealthIndicator } from './health/kafka-health-indicator';

// Core infrastructure providers - consumer services are now in ConsumerModule
const CORE_PROVIDERS: Provider[] = [
  KafkaCoreService,
  KafkaClient,
  KafkaHealthIndicator,
  TracingService,
];

@Global()
@Module({})
export class KafkaModule {
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
      global: true,
      providers,
      exports: [
        KAFKA_MODULE_OPTIONS,
        optionsToken,
        clientToken,
        KafkaCoreService,
        KafkaClient,
        KafkaHealthIndicator,
        TracingService,
      ],
    };
  }

  /**
   * Register multiple Kafka connections at once
   */
  static forRootMultiple(optionsArray: KafkaModuleOptions[]): DynamicModule {
    const providers: Provider[] = [];
    const exports: (string | symbol | Provider)[] = [
      KAFKA_MODULE_OPTIONS,
      KafkaCoreService,
      KafkaClient,
      KafkaHealthIndicator,
      TracingService,
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
        KAFKA_MODULE_OPTIONS,
        optionsToken,
        clientToken,
        KafkaCoreService,
        KafkaClient,
        KafkaHealthIndicator,
        TracingService,
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
