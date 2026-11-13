import { DynamicModule, Global, Module, Provider } from '@nestjs/common';
import {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  KafkaOptionsFactory,
  KAFKA_MODULE_OPTIONS,
  DEFAULT_KAFKA_CONNECTION,
  getKafkaOptionsToken,
  getKafkaClientToken,
} from './interfaces';
import { KafkaCoreModule } from './kafka-core.module';
import { KafkaCoreService } from './services/kafka-core.service';
import {
  KafkaClient,
  ConnectionBoundClient,
} from './services/kafka-client.service';
import { TracingService } from './services/tracing.service';

@Global()
@Module({})
export class KafkaModule {
  /**
   * Register one Kafka connection. Call once per connection, each with its own
   * `name`; all connections share one KafkaCoreService. A second call with the
   * same name and different options fails at startup.
   */
  static forRoot(options: KafkaModuleOptions): DynamicModule {
    const name = options.name || DEFAULT_KAFKA_CONNECTION;
    return this.build(
      [name],
      [{ provide: getKafkaOptionsToken(name), useValue: options }],
    );
  }

  /**
   * Register a Kafka connection asynchronously
   */
  static forRootAsync(options: KafkaModuleAsyncOptions): DynamicModule {
    const name = options.name || DEFAULT_KAFKA_CONNECTION;
    const module = this.build(
      [name],
      this.createAsyncProviders(options, getKafkaOptionsToken(name)),
    );
    module.imports = [...(module.imports ?? []), ...(options.imports || [])];
    module.global = options.global ?? true;
    return module;
  }

  /**
   * Per-connection providers on top of the shared KafkaCoreModule.
   * `names[0]` backs KAFKA_MODULE_OPTIONS.
   */
  private static build(
    names: string[],
    optionsProviders: Provider[],
  ): DynamicModule {
    const optionsTokens = names.map((n) => getKafkaOptionsToken(n));
    const clientTokens = names.map((n) => getKafkaClientToken(n));

    return {
      module: KafkaModule,
      global: true,
      imports: [KafkaCoreModule],
      providers: [
        ...optionsProviders,
        { provide: KAFKA_MODULE_OPTIONS, useExisting: optionsTokens[0] },
        ...names.flatMap((n) => this.connectionProviders(n)),
      ],
      exports: [
        KafkaCoreModule,
        KAFKA_MODULE_OPTIONS,
        ...optionsTokens,
        ...clientTokens,
      ],
    };
  }

  /** Registers the connection and exposes its client token. */
  private static connectionProviders(name: string): Provider[] {
    return [
      {
        provide: `KAFKA_INIT_${name}`,
        useFactory: (
          kafkaCore: KafkaCoreService,
          tracing: TracingService,
          options: KafkaModuleOptions,
        ) => {
          kafkaCore.registerConnection({ ...options, name });
          tracing.configure(options.tracing);
          return true;
        },
        inject: [KafkaCoreService, TracingService, getKafkaOptionsToken(name)],
      },
      {
        provide: getKafkaClientToken(name),
        useFactory: (
          kafkaClient: KafkaClient,
        ): KafkaClient | ConnectionBoundClient =>
          name === DEFAULT_KAFKA_CONNECTION
            ? kafkaClient
            : new ConnectionBoundClient(kafkaClient, name),
        inject: [KafkaClient],
      },
    ];
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
      ...(options.useClass ? [{ provide: useClass, useClass }] : []),
    ];
  }
}
