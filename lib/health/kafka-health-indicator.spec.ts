import { Test, TestingModule } from '@nestjs/testing';
import { KafkaHealthIndicator } from './kafka-health-indicator';
import { KafkaClient } from '../services/kafka-client.service';
import { KafkaCoreService } from '../services/kafka-core.service';

describe('KafkaHealthIndicator', () => {
  let healthIndicator: KafkaHealthIndicator;
  let kafkaClient: jest.Mocked<KafkaClient>;
  let kafkaCore: jest.Mocked<KafkaCoreService>;

  beforeEach(async () => {
    const mockKafkaClient = {
      isHealthy: jest.fn(),
    };

    const mockAdminClient = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      listTopics: jest.fn(),
      fetchOffsets: jest.fn(),
    };

    const mockKafkaCore = {
      getKafka: jest.fn().mockReturnValue({
        admin: jest.fn().mockReturnValue(mockAdminClient),
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        KafkaHealthIndicator,
        { provide: KafkaClient, useValue: mockKafkaClient },
        { provide: KafkaCoreService, useValue: mockKafkaCore },
      ],
    }).compile();

    healthIndicator = module.get<KafkaHealthIndicator>(KafkaHealthIndicator);
    kafkaClient = module.get(KafkaClient);
    kafkaCore = module.get(KafkaCoreService);
  });

  describe('isHealthy', () => {
    it('should return up status when producer is connected', () => {
      kafkaClient.isHealthy.mockReturnValue(true);

      const result = healthIndicator.isHealthy('kafka');

      expect(result).toEqual({
        kafka: {
          status: 'up',
          connected: true,
        },
      });
    });

    it('should return down status when producer is not connected', () => {
      kafkaClient.isHealthy.mockReturnValue(false);

      const result = healthIndicator.isHealthy('kafka');

      expect(result).toEqual({
        kafka: {
          status: 'down',
          connected: false,
          message: 'Kafka producer is not connected',
        },
      });
    });
  });

  describe('checkBrokers', () => {
    it('should return connected info when brokers are healthy', async () => {
      const mockTopics = ['topic-1', 'topic-2', 'topic-3'];

      const admin = kafkaCore.getKafka().admin();
      (admin.listTopics as jest.Mock).mockResolvedValue(mockTopics);

      const result = await healthIndicator.checkBrokers('kafka-brokers');

      expect(result).toEqual({
        'kafka-brokers': {
          status: 'up',
          connected: true,
          topicCount: 3,
        },
      });
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(admin.connect).toHaveBeenCalled();
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(admin.disconnect).toHaveBeenCalled();
    });

    it('should return down status when broker check fails', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.connect as jest.Mock).mockRejectedValue(
        new Error('Connection failed'),
      );

      const result = await healthIndicator.checkBrokers('kafka-brokers');

      expect(result).toEqual({
        'kafka-brokers': {
          status: 'down',
          error: 'Connection failed',
        },
      });
    });
  });

  describe('checkConsumerLag', () => {
    it('should return up when lag is below threshold', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.fetchOffsets as jest.Mock).mockResolvedValue([
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, offset: '100' }],
        },
      ]);

      const result = await healthIndicator.checkConsumerLag(
        'lag-check',
        'test-group',
        1000,
      );

      expect(result).toEqual({
        'lag-check': {
          status: 'up',
          groupId: 'test-group',
          lag: 100,
          maxLag: 1000,
        },
      });
    });

    it('should return down when lag exceeds threshold', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.fetchOffsets as jest.Mock).mockResolvedValue([
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, offset: '5000' }],
        },
      ]);

      const result = await healthIndicator.checkConsumerLag(
        'lag-check',
        'test-group',
        1000,
      );

      expect(result).toEqual({
        'lag-check': {
          status: 'down',
          groupId: 'test-group',
          lag: 5000,
          maxLag: 1000,
        },
      });
    });

    it('should return down when fetch fails', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.connect as jest.Mock).mockRejectedValue(new Error('Fetch failed'));

      const result = await healthIndicator.checkConsumerLag(
        'lag-check',
        'test-group',
      );

      expect(result).toEqual({
        'lag-check': {
          status: 'down',
          error: 'Fetch failed',
        },
      });
    });
  });

  describe('without TerminusModule (fallback)', () => {
    it('should work without HealthIndicatorService', () => {
      // The test setup doesn't provide HealthIndicatorService
      // so it should use the fallback implementation
      kafkaClient.isHealthy.mockReturnValue(true);

      const result = healthIndicator.isHealthy('kafka');

      expect(result.kafka.status).toBe('up');
    });
  });
});
