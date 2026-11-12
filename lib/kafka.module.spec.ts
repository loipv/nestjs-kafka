import { Test, TestingModule } from '@nestjs/testing';
import { KafkaModule } from './kafka.module';
import { KafkaCoreService } from './services/kafka-core.service';
import {
  ConnectionBoundClient,
  KafkaClient,
} from './services/kafka-client.service';
import { TracingService } from './services/tracing.service';
import { KafkaHealthIndicator } from './health/kafka-health-indicator';

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

describe('KafkaModule', () => {
  describe('forRoot', () => {
    let module: TestingModule;

    beforeEach(async () => {
      module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            clientId: 'test-client',
            brokers: ['localhost:9092'],
          }),
        ],
      }).compile();
    });

    afterEach(async () => {
      await module.close();
    });

    it('should provide KafkaCoreService', () => {
      const service = module.get<KafkaCoreService>(KafkaCoreService);
      expect(service).toBeDefined();
    });

    it('should provide KafkaClient', () => {
      const client = module.get<KafkaClient>(KafkaClient);
      expect(client).toBeDefined();
    });

    it('should provide KafkaHealthIndicator', () => {
      const health = module.get<KafkaHealthIndicator>(KafkaHealthIndicator);
      expect(health).toBeDefined();
    });

    it('should NOT provide ConsumerRegistryService (moved to ConsumerModule)', () => {
      expect(() => {
        module.get('ConsumerRegistryService');
      }).toThrow();
    });

    it('should NOT provide BatchProcessorService (moved to ConsumerModule)', () => {
      expect(() => {
        module.get('BatchProcessorService');
      }).toThrow();
    });

    it('should register connection', () => {
      const coreService = module.get<KafkaCoreService>(KafkaCoreService);
      expect(coreService.hasConnection()).toBe(true);
    });
  });

  describe('several connections share one core', () => {
    const A = { clientId: 'a', brokers: ['localhost:9092'] };
    const B = { name: 'analytics', clientId: 'b', brokers: ['localhost:9093'] };
    const names = (m: TestingModule) =>
      m.get(KafkaCoreService, { strict: false }).getConnectionNames().sort();
    const tracingEnabled = (m: TestingModule) =>
      m.get(TracingService, { strict: false }).isEnabled();
    let m: TestingModule | undefined;

    const compile = async (imports: any[]) => {
      m = await Test.createTestingModule({ imports }).compile();
      return m;
    };

    afterEach(async () => {
      await m?.close();
      m = undefined;
    });

    it('two forRoot() calls register into one KafkaCoreService', async () => {
      const mod = await compile([
        KafkaModule.forRoot(A),
        KafkaModule.forRoot(B),
      ]);
      expect(names(mod)).toEqual(['analytics', 'default']);
      expect(
        mod.get('KAFKA_CLIENT_analytics', { strict: false }),
      ).toBeInstanceOf(ConnectionBoundClient);
      expect(mod.get('KAFKA_CLIENT_default', { strict: false })).toBe(
        mod.get(KafkaClient, { strict: false }),
      );
    });

    it('forRootMultiple registers every connection', async () => {
      const mod = await compile([KafkaModule.forRootMultiple([A, B])]);
      expect(names(mod)).toEqual(['analytics', 'default']);
    });

    it('two forRootAsync() calls register into one KafkaCoreService', async () => {
      const mod = await compile([
        KafkaModule.forRootAsync({ useFactory: () => A }),
        KafkaModule.forRootAsync({ name: 'analytics', useFactory: () => B }),
      ]);
      expect(names(mod)).toEqual(['analytics', 'default']);
    });

    it('two unnamed forRoot() calls boot with one connection', async () => {
      const mod = await compile([
        KafkaModule.forRoot(A),
        KafkaModule.forRoot({ ...A, clientId: 'a2' }),
      ]);
      expect(names(mod)).toEqual(['default']);
    });

    it('enables tracing when only the second connection asks for it', async () => {
      const mod = await compile([
        KafkaModule.forRoot(A),
        KafkaModule.forRoot({ ...B, tracing: { enabled: true } }),
      ]);
      expect(tracingEnabled(mod)).toBe(true);
    });

    it('enables tracing configured through forRootAsync', async () => {
      const mod = await compile([
        KafkaModule.forRootAsync({
          useFactory: () => ({ ...A, tracing: { enabled: true } }),
        }),
      ]);
      expect(tracingEnabled(mod)).toBe(true);
    });
  });

  describe('forRootAsync', () => {
    it('should work with useFactory', async () => {
      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRootAsync({
            useFactory: () => ({
              clientId: 'async-test-client',
              brokers: ['localhost:9092'],
            }),
          }),
        ],
      }).compile();

      const service = module.get<KafkaCoreService>(KafkaCoreService);
      expect(service).toBeDefined();

      await module.close();
    });
  });
});
