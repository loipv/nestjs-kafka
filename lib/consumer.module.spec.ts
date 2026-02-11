import { Test, TestingModule } from '@nestjs/testing';
import { Injectable } from '@nestjs/common';
import { ConsumerModule } from './consumer.module';
import { KafkaModule } from './kafka.module';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';
import { BatchProcessorService } from './services/batch-processor.service';
import { IdempotencyService } from './services/idempotency.service';
import { PressureManagerService } from './services/pressure-manager.service';
import { DlqService } from './services/dlq.service';
import { DlqRetryService } from './services/dlq-retry.service';
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
        run: jest.fn(),
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

// Test consumer class
@Injectable()
class TestConsumer {
  @Consumer('test-topic')
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async handleMessage(message: any) {
    // Process message
  }
}

describe('ConsumerModule', () => {
  describe('forRoot', () => {
    let module: TestingModule;

    beforeEach(async () => {
      // Clear static state between tests
      ConsumerModule.clearConsumers();

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

    it('should provide PressureManagerService', () => {
      const service = module.get<PressureManagerService>(
        PressureManagerService,
      );
      expect(service).toBeDefined();
    });

    it('should provide DlqService', () => {
      const service = module.get<DlqService>(DlqService);
      expect(service).toBeDefined();
    });

    it('should provide DlqRetryService', () => {
      const service = module.get<DlqRetryService>(DlqRetryService);
      expect(service).toBeDefined();
    });
  });

  describe('forFeature', () => {
    let module: TestingModule;

    beforeEach(async () => {
      ConsumerModule.clearConsumers();

      module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            clientId: 'test-client',
            brokers: ['localhost:9092'],
          }),
          ConsumerModule.forRoot(),
          ConsumerModule.forFeature([TestConsumer]),
        ],
        providers: [TestConsumer],
      }).compile();
    });

    afterEach(async () => {
      await module.close();
    });

    it('should collect consumer instances', () => {
      const instances = ConsumerModule.getConsumerInstances();
      expect(instances.length).toBeGreaterThan(0);
      expect(instances.some((i) => i instanceof TestConsumer)).toBe(true);
    });

    it('should export consumer class', () => {
      const consumer = module.get<TestConsumer>(TestConsumer);
      expect(consumer).toBeDefined();
    });
  });

  describe('module separation', () => {
    it('should require KafkaModule for consumer services to work', async () => {
      // ConsumerModule depends on KafkaCoreService and KafkaClient from KafkaModule
      // This test verifies the dependency chain
      ConsumerModule.clearConsumers();

      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            clientId: 'test-client',
            brokers: ['localhost:9092'],
          }),
          ConsumerModule.forRoot(),
        ],
      }).compile();

      // ConsumerRegistryService should be able to inject KafkaCoreService
      const registry = module.get<ConsumerRegistryService>(
        ConsumerRegistryService,
      );
      expect(registry).toBeDefined();

      await module.close();
    });
  });
});
