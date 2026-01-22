import { Test, TestingModule } from '@nestjs/testing';
import { KafkaModule } from './kafka.module';
import { KafkaCoreService } from './services/kafka-core.service';
import { KafkaClient } from './services/kafka-client.service';
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
