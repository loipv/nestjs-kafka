import { Test, TestingModule } from '@nestjs/testing';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { Injectable, Logger, Module, Scope } from '@nestjs/common';
import { ConsumerModule } from './consumer.module';
import { KafkaModule } from './kafka.module';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { DlqService } from './services/dlq.service';
import { Consumer } from './decorators/consumer.decorator';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript');

// A dependency service that TestConsumer injects
@Injectable()
class TestDependencyService {
  getData(): string {
    return 'test-data';
  }
}

// Test consumer class that injects a dependency service
@Injectable()
class TestConsumer {
  constructor(private readonly testDependency: TestDependencyService) {}

  @Consumer('test-topic')
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async handleMessage(message: any) {
    // Process message using dependency
    this.testDependency.getData();
  }
}

// A service without @Consumer (should NOT be discovered)
@Injectable()
class RegularService {
  doSomething(): string {
    return 'not a consumer';
  }
}

describe('ConsumerModule', () => {
  describe('forRoot', () => {
    let module: TestingModule;

    beforeEach(async () => {
      module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            clientId: 'test-client',
            brokers: ['localhost:9092'],
          }),
          ConsumerModule.forRoot(),
        ],
      }).compile();
    });

    afterEach(async () => {
      await module.close();
    });

    it('provides ConsumerDiscoveryService', () => {
      const service = module.get<ConsumerDiscoveryService>(
        ConsumerDiscoveryService,
      );
      expect(service).toBeDefined();
    });

    it('provides ConsumerRegistryService', () => {
      const service = module.get<ConsumerRegistryService>(
        ConsumerRegistryService,
      );
      expect(service).toBeDefined();
    });

    it('provides BatchProcessorService', () => {
      const service = module.get<BatchProcessorService>(BatchProcessorService);
      expect(service).toBeDefined();
    });

    it('provides IdempotencyService', () => {
      const service = module.get<IdempotencyService>(IdempotencyService);
      expect(service).toBeDefined();
    });

    it('provides DlqService', () => {
      const service = module.get<DlqService>(DlqService);
      expect(service).toBeDefined();
    });
  });

  it('onApplicationShutdown does not call gracefulShutdown (the registry owns it)', () => {
    const registry = { gracefulShutdown: jest.fn() };
    const mod = new ConsumerModule({} as any, registry as any, {} as any);
    mod.onApplicationShutdown();
    expect(registry.gracefulShutdown).not.toHaveBeenCalled();
  });

  it("starts consumers after every provider's onModuleInit", async () => {
    let ready = false;
    @Injectable()
    class WarmUp {
      onModuleInit() {
        ready = true;
      }
    }
    const seen: boolean[] = [];
    const startAll = jest
      .spyOn(ConsumerRegistryService.prototype, 'startAll')
      .mockImplementation(() => {
        seen.push(ready);
        return Promise.resolve();
      });
    const mod = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ clientId: 'c', brokers: ['localhost:9092'] }),
        ConsumerModule.forRoot(),
      ],
      providers: [TestDependencyService, TestConsumer, WarmUp],
    }).compile();
    try {
      await mod.init();
      expect(seen).toEqual([true]);
      expect((mod.get(ConsumerModule) as any).onModuleInit).toBeUndefined();
    } finally {
      startAll.mockRestore();
      await mod.close();
    }
  });

  it('aborts startup (closing consumers and producers) when consumers fail to start', async () => {
    const registry = {
      registerConsumers: jest.fn(() => {
        throw new Error('Kafka connection "missing" not found');
      }),
      startAll: jest.fn(),
      abortStartup: jest.fn().mockResolvedValue(undefined),
    };
    const discovery = {
      discoverFromProviders: jest.fn(() => [{ topic: 't' }]),
      warnSkipped: jest.fn(),
    };
    const nestDiscovery = { getProviders: () => [] };
    const mod = new ConsumerModule(
      discovery as any,
      registry as any,
      nestDiscovery as any,
    );

    await expect(mod.onApplicationBootstrap()).rejects.toThrow(
      'Kafka connection "missing" not found',
    );
    expect(registry.abortStartup).toHaveBeenCalledTimes(1);
  });

  it('boots when a consumer provider is also exposed through useExisting', async () => {
    @Injectable()
    class Orders {
      @Consumer('orders')
      async handle() {}
    }
    @Module({
      providers: [Orders, { provide: 'ORDERS_ALIAS', useExisting: Orders }],
      exports: ['ORDERS_ALIAS'],
    })
    class FeatureModule {}

    const startAll = jest
      .spyOn(ConsumerRegistryService.prototype, 'startAll')
      .mockResolvedValue();
    const mod = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ clientId: 'c', brokers: ['localhost:9092'] }),
        ConsumerModule.forRoot(),
        FeatureModule,
      ],
    }).compile();
    try {
      await mod.init();
      const groups = (mod.get(ConsumerRegistryService) as any).consumerGroups;
      expect([...groups.keys()]).toEqual(['default:orders-group']);
      expect([...groups.get('default:orders-group').topics.keys()]).toEqual([
        'orders',
      ]);
    } finally {
      startAll.mockRestore();
      await mod.close();
    }
  });

  it('skips a request-scoped consumer provider with a warning', async () => {
    @Injectable({ scope: Scope.REQUEST })
    class Scoped {
      @Consumer('scoped')
      async h() {}
    }
    const warn = jest
      .spyOn(Logger.prototype, 'warn')
      .mockImplementation(() => undefined);
    const startAll = jest
      .spyOn(ConsumerRegistryService.prototype, 'startAll')
      .mockResolvedValue();
    const mod = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ clientId: 'c', brokers: ['localhost:9092'] }),
        ConsumerModule.forRoot(),
      ],
      providers: [Scoped],
    }).compile();
    try {
      await mod.init();
      const groups = (mod.get(ConsumerRegistryService) as any).consumerGroups;
      expect([...groups.keys()]).toEqual([]);
      expect(warn).toHaveBeenCalledWith(
        'Skipping consumer Scoped.h: request-scoped providers cannot consume',
      );
    } finally {
      warn.mockRestore();
      startAll.mockRestore();
      await mod.close();
    }
  });

  it.each([
    ['KafkaModule first', true],
    ['ConsumerModule first', false],
  ])('stops consumers before producers (%s)', async (_order, kafkaFirst) => {
    @Injectable()
    class OrderConsumer {
      @Consumer('ordering')
      async handle() {}
    }
    const kafka = KafkaModule.forRoot({
      clientId: 'c',
      brokers: ['localhost:9092'],
    });
    const consumers = ConsumerModule.forRoot();
    const mod = await Test.createTestingModule({
      imports: kafkaFirst ? [kafka, consumers] : [consumers, kafka],
      providers: [OrderConsumer],
    }).compile();
    await mod.init();
    await new Promise((r) => setImmediate(r)); // bootstrap producer connect

    const client = (KafkaJS.Kafka as unknown as jest.Mock).mock.results.at(
      -1,
    )!.value;
    const producer = client.producer.mock.results.at(-1).value;
    const consumer = client.consumer.mock.results.at(-1).value;
    await mod.close();

    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
    expect(producer.disconnect).toHaveBeenCalledTimes(1);
    expect(consumer.disconnect.mock.invocationCallOrder[0]).toBeLessThan(
      producer.disconnect.mock.invocationCallOrder[0],
    );
  });

  describe('autoStart: false', () => {
    @Injectable()
    class Later {
      @Consumer('later')
      async handle() {}
    }
    const compileWith = (providers: any[]) =>
      Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({ clientId: 'c', brokers: ['localhost:9092'] }),
          ConsumerModule.forRoot({ autoStart: false }),
        ],
        providers,
      }).compile();
    const lastClient = () =>
      (KafkaJS.Kafka as unknown as jest.Mock).mock.results.at(-1)!.value;

    it('registers but does not start', async () => {
      const mod = await compileWith([Later]);
      try {
        await mod.init();
        const client = lastClient();
        expect(client.consumer).not.toHaveBeenCalled();
        const registry = mod.get(ConsumerRegistryService);
        expect([...(registry as any).consumerGroups.keys()]).toEqual([
          'default:later-group',
        ]);
        await registry.startAll();
        expect(client.consumer.mock.results[0].value.run).toHaveBeenCalledTimes(
          1,
        );
      } finally {
        await mod.close();
      }
    });

    it('still fails init on an invalid option', async () => {
      @Injectable()
      class Invalid {
        @Consumer('invalid', { retry: { multiplier: 0 } })
        async handle() {}
      }
      const mod = await compileWith([Invalid]);
      await expect(mod.init()).rejects.toThrow(
        'Consumer "invalid": retry.multiplier must be a finite number >= 1',
      );
    });

    it('closes cleanly without startAll', async () => {
      const mod = await compileWith([Later]);
      await mod.init();
      await expect(mod.close()).resolves.toBeUndefined();
      expect(lastClient().consumer).not.toHaveBeenCalled();
    });
  });

  it('no longer exposes the static clearConsumers()', () => {
    expect((ConsumerModule as any).clearConsumers).toBeUndefined();
  });

  describe('auto-discovery', () => {
    let module: TestingModule;

    beforeEach(async () => {
      module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            clientId: 'test-client',
            brokers: ['localhost:9092'],
          }),
          ConsumerModule.forRoot(),
        ],
        // Just declare consumer and its dependencies as providers.
        // No forFeature() needed — auto-discovery handles everything.
        providers: [TestConsumer, TestDependencyService, RegularService],
      }).compile();

      // Initialize to trigger lifecycle hooks (onModuleInit)
      // which triggers auto-discovery of @Consumer() decorated methods
      await module.init();
    });

    afterEach(async () => {
      await module.close();
    });

    it('auto-discovers a consumer via @Consumer', () => {
      const discoveryService = module.get<ConsumerDiscoveryService>(
        ConsumerDiscoveryService,
      );
      const consumers = discoveryService.getConsumers();
      expect(consumers.length).toBeGreaterThan(0);
      expect(consumers.some((c) => c.target instanceof TestConsumer)).toBe(
        true,
      );
    });

    it('resolves a consumer with its injected dependencies', () => {
      const consumer = module.get<TestConsumer>(TestConsumer);
      expect(consumer).toBeDefined();
      // TestConsumer should have TestDependencyService injected successfully
      expect(consumer['testDependency']).toBeDefined();
      expect(consumer['testDependency']).toBeInstanceOf(TestDependencyService);
    });

    it('does not discover non-consumer providers', () => {
      const discoveryService = module.get<ConsumerDiscoveryService>(
        ConsumerDiscoveryService,
      );
      const consumers = discoveryService.getConsumers();
      // RegularService has no @Consumer decorator — should not be discovered
      expect(consumers.some((c) => c.target instanceof RegularService)).toBe(
        false,
      );
    });

    it('discovers the topic from @Consumer metadata', () => {
      const discoveryService = module.get<ConsumerDiscoveryService>(
        ConsumerDiscoveryService,
      );
      const consumers = discoveryService.getConsumers();
      const testConsumer = consumers.find(
        (c) => c.target instanceof TestConsumer,
      );
      expect(testConsumer).toBeDefined();
      expect(testConsumer!.topic).toBe('test-topic');
    });
  });

  describe('module separation', () => {
    it('resolves consumer services with KafkaModule imported', async () => {
      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            clientId: 'test-client',
            brokers: ['localhost:9092'],
          }),
          ConsumerModule.forRoot(),
        ],
      }).compile();

      const registry = module.get<ConsumerRegistryService>(
        ConsumerRegistryService,
      );
      expect(registry).toBeDefined();

      await module.close();
    });
  });
});
