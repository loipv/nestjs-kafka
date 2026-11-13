import { Injectable, Module } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { KafkaModule } from './kafka.module';
import { KafkaOptionsFactory } from './interfaces';
import { KafkaCoreService } from './services/kafka-core.service';
import {
  ConnectionBoundClient,
  KafkaClient,
} from './services/kafka-client.service';
import { TracingService } from './services/tracing.service';
import { KafkaHealthIndicator } from './health/kafka-health-indicator';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript');

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

    it('provides KafkaCoreService', () => {
      const service = module.get<KafkaCoreService>(KafkaCoreService);
      expect(service).toBeDefined();
    });

    it('provides KafkaClient', () => {
      const client = module.get<KafkaClient>(KafkaClient);
      expect(client).toBeDefined();
    });

    it('provides KafkaHealthIndicator', () => {
      const health = module.get<KafkaHealthIndicator>(KafkaHealthIndicator);
      expect(health).toBeDefined();
    });

    it('does not provide ConsumerRegistryService (it lives in ConsumerModule)', () => {
      expect(() => {
        module.get('ConsumerRegistryService');
      }).toThrow();
    });

    it('does not provide BatchProcessorService (it lives in ConsumerModule)', () => {
      expect(() => {
        module.get('BatchProcessorService');
      }).toThrow();
    });

    it('registers the connection', () => {
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

    it('no longer exports KAFKA_MODULE_OPTIONS', async () => {
      expect(await import('./index')).not.toHaveProperty(
        'KAFKA_MODULE_OPTIONS',
      );
    });

    it('no longer exposes forRootMultiple', () => {
      expect((KafkaModule as any).forRootMultiple).toBeUndefined();
    });

    it('two forRootAsync() calls register into one KafkaCoreService', async () => {
      const mod = await compile([
        KafkaModule.forRootAsync({ useFactory: () => A }),
        KafkaModule.forRootAsync({ name: 'analytics', useFactory: () => B }),
      ]);
      expect(names(mod)).toEqual(['analytics', 'default']);
    });

    it('fails when two forRoot() calls use one name with different options', async () => {
      await expect(
        compile([
          KafkaModule.forRoot(A),
          KafkaModule.forRoot({ ...A, clientId: 'a2' }),
        ]),
      ).rejects.toThrow('Kafka connection "default" is already registered');
    });

    it('collapses two forRoot() calls with identical options', async () => {
      const mod = await compile([
        KafkaModule.forRoot(A),
        KafkaModule.forRoot(A),
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

  describe('forRootAsync with a class', () => {
    let constructed = 0;

    @Injectable()
    class KafkaConfig implements KafkaOptionsFactory {
      constructor() {
        constructed++;
      }
      createKafkaOptions() {
        return { clientId: 'cls', brokers: ['localhost:9092'] };
      }
    }

    @Module({ providers: [KafkaConfig], exports: [KafkaConfig] })
    class ConfigModule {}

    let m: TestingModule | undefined;
    beforeEach(() => {
      constructed = 0;
    });
    afterEach(async () => {
      await m?.close();
      m = undefined;
    });

    it('registers the connection with useClass', async () => {
      m = await Test.createTestingModule({
        imports: [KafkaModule.forRootAsync({ useClass: KafkaConfig })],
      }).compile();
      expect(
        m.get(KafkaCoreService, { strict: false }).getConnectionNames(),
      ).toEqual(['default']);
      expect(constructed).toBe(1);
    });

    it('reuses the imported factory with useExisting', async () => {
      m = await Test.createTestingModule({
        imports: [
          KafkaModule.forRootAsync({
            imports: [ConfigModule],
            useExisting: KafkaConfig,
          }),
        ],
      }).compile();
      expect(
        m.get(KafkaCoreService, { strict: false }).getConnectionNames(),
      ).toEqual(['default']);
      expect(constructed).toBe(1);
    });
  });

  describe('forRootAsync', () => {
    it('works with useFactory', async () => {
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
