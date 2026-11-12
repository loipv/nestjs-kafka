import { Test, TestingModule } from '@nestjs/testing';
import { Injectable } from '@nestjs/common';
import { ConsumerModule } from './consumer.module';
import { KafkaModule } from './kafka.module';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { DlqService } from './services/dlq.service';
import { Consumer } from './decorators/consumer.decorator';

// Mock @confluentinc/kafka-javascript
jest.mock('@confluentinc/kafka-javascript', () => ({
  KafkaJS: {
    Kafka: jest.fn().mockImplementation(() => ({
      producer: jest.fn().mockReturnValue({
        connect: jest.fn(),
        disconnect: jest.fn(),
        send: jest.fn(),
      }),
      consumer: jest.fn().mockReturnValue({
        connect: jest.fn(),
        disconnect: jest.fn(),
        subscribe: jest.fn(),
        run: jest.fn().mockResolvedValue(undefined),
      }),
      admin: jest.fn().mockReturnValue({
        connect: jest.fn(),
        disconnect: jest.fn(),
        listTopics: jest.fn().mockResolvedValue([]),
      }),
    })),
    logLevel: {
      NOTHING: 0,
      ERROR: 1,
      WARN: 2,
      INFO: 4,
      DEBUG: 5,
    },
  },
}));

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

    it('should provide ConsumerDiscoveryService', () => {
      const service = module.get<ConsumerDiscoveryService>(
        ConsumerDiscoveryService,
      );
      expect(service).toBeDefined();
    });

    it('should provide ConsumerRegistryService', () => {
      const service = module.get<ConsumerRegistryService>(
        ConsumerRegistryService,
      );
      expect(service).toBeDefined();
    });

    it('should provide BatchProcessorService', () => {
      const service = module.get<BatchProcessorService>(BatchProcessorService);
      expect(service).toBeDefined();
    });

    it('should provide IdempotencyService', () => {
      const service = module.get<IdempotencyService>(IdempotencyService);
      expect(service).toBeDefined();
    });

    it('should provide DlqService', () => {
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
      discoverFromProviders: jest.fn(),
      getConsumers: () => [{ topic: 't' }],
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

    it('should auto-discover consumer via @Consumer decorator', () => {
      const discoveryService = module.get<ConsumerDiscoveryService>(
        ConsumerDiscoveryService,
      );
      const consumers = discoveryService.getConsumers();
      expect(consumers.length).toBeGreaterThan(0);
      expect(consumers.some((c) => c.target instanceof TestConsumer)).toBe(
        true,
      );
    });

    it('should resolve consumer with its injected dependencies', () => {
      const consumer = module.get<TestConsumer>(TestConsumer);
      expect(consumer).toBeDefined();
      // TestConsumer should have TestDependencyService injected successfully
      expect(consumer['testDependency']).toBeDefined();
      expect(consumer['testDependency']).toBeInstanceOf(TestDependencyService);
    });

    it('should NOT discover non-consumer providers', () => {
      const discoveryService = module.get<ConsumerDiscoveryService>(
        ConsumerDiscoveryService,
      );
      const consumers = discoveryService.getConsumers();
      // RegularService has no @Consumer decorator — should not be discovered
      expect(consumers.some((c) => c.target instanceof RegularService)).toBe(
        false,
      );
    });

    it('should discover the correct topic from @Consumer metadata', () => {
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
    it('should require KafkaModule for consumer services to work', async () => {
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
