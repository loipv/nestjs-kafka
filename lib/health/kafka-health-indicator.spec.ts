import { ServiceUnavailableException } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { HealthCheckService, TerminusModule } from '@nestjs/terminus';
import { KafkaHealthIndicator } from './kafka-health-indicator';
import { KafkaCoreService } from '../services/kafka-core.service';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript');

describe('KafkaHealthIndicator', () => {
  let healthIndicator: KafkaHealthIndicator;
  let kafkaCore: jest.Mocked<KafkaCoreService>;
  let producers: Record<string, boolean>;
  let consumers: Record<string, string>;

  beforeEach(async () => {
    producers = { default: true };
    consumers = {};

    const mockAdminClient = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      listTopics: jest.fn(),
      fetchOffsets: jest.fn(),
      fetchTopicOffsets: jest.fn(),
    };

    const mockKafkaCore = {
      getAdmin: jest.fn().mockResolvedValue(mockAdminClient),
      getKafka: jest.fn().mockReturnValue({
        admin: jest.fn().mockReturnValue(mockAdminClient),
      }),
      getConnectionNames: jest.fn(() => Object.keys(producers)),
      isProducerConnected: jest.fn((name: string) => producers[name]),
      getConsumerStatuses: jest.fn(() => ({ ...consumers })),
      connectProducerInBackground: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        KafkaHealthIndicator,
        { provide: KafkaCoreService, useValue: mockKafkaCore },
      ],
    }).compile();

    healthIndicator = module.get<KafkaHealthIndicator>(KafkaHealthIndicator);
    kafkaCore = module.get(KafkaCoreService);
  });

  describe('isHealthy', () => {
    it('is up when every producer is connected and every group is running', () => {
      producers = { default: true, analytics: true };
      consumers = { 'default:g': 'running' };

      expect(healthIndicator.isHealthy('kafka')).toEqual({
        kafka: {
          status: 'up',
          producers: { default: true, analytics: true },
          consumers: { 'default:g': 'running' },
        },
      });
    });

    it('is down with a message naming a disconnected producer', () => {
      producers = { default: true, analytics: false };

      expect(healthIndicator.isHealthy('kafka')).toEqual({
        kafka: {
          status: 'down',
          producers: { default: true, analytics: false },
          consumers: {},
          message: 'Not ready: producer "analytics" not connected',
        },
      });
    });

    it('starts a background reconnect for a disconnected producer', () => {
      producers = { default: true, analytics: false };
      healthIndicator.isHealthy('kafka');
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(kafkaCore.connectProducerInBackground).toHaveBeenCalledTimes(1);
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(kafkaCore.connectProducerInBackground).toHaveBeenCalledWith(
        'analytics',
      );
    });

    it('is down when a group is failed', () => {
      consumers = { 'default:g': 'failed', 'default:h': 'running' };

      const result = healthIndicator.isHealthy('kafka');

      expect(result.kafka.status).toBe('down');
      expect(result.kafka.message).toBe(
        'Not ready: consumer group "default:g" is failed',
      );
    });

    it('joins several problems', () => {
      producers = { default: false };
      consumers = { 'default:g': 'starting' };

      expect(healthIndicator.isHealthy('kafka').kafka.message).toBe(
        'Not ready: producer "default" not connected; consumer group "default:g" is starting',
      );
    });
  });

  describe('checkBrokers', () => {
    it('returns connected info when brokers are healthy', async () => {
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
      expect(kafkaCore.getAdmin).toHaveBeenCalledWith(undefined);
      // The shared admin stays connected between probes
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(admin.disconnect).not.toHaveBeenCalled();
    });

    it('returns down when the broker check fails', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.listTopics as jest.Mock).mockRejectedValue(
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
    it('returns up when lag is below the threshold', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.fetchOffsets as jest.Mock).mockResolvedValue([
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, offset: '0' }],
        },
      ]);
      (admin.fetchTopicOffsets as jest.Mock).mockResolvedValue([
        { partition: 0, offset: '100' },
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

    it('returns down when lag exceeds the threshold', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.fetchOffsets as jest.Mock).mockResolvedValue([
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, offset: '0' }],
        },
      ]);
      (admin.fetchTopicOffsets as jest.Mock).mockResolvedValue([
        { partition: 0, offset: '5000' },
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

    it('counts a topic whose latest offsets cannot be fetched as lag 0', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.fetchOffsets as jest.Mock).mockResolvedValue([
        { topic: 'gone', partitions: [{ partition: 0, offset: '0' }] },
        { topic: 'ok', partitions: [{ partition: 0, offset: '0' }] },
      ]);
      (admin.fetchTopicOffsets as jest.Mock).mockImplementation((t: string) =>
        t === 'gone'
          ? Promise.reject(new Error('unknown topic'))
          : Promise.resolve([{ partition: 0, offset: '7' }]),
      );

      const result = await healthIndicator.checkConsumerLag('lag', 'g', 1000);

      expect(result).toEqual({
        lag: {
          status: 'up',
          groupId: 'g',
          lag: 7,
          maxLag: 1000,
          unreadableTopics: ['gone'],
        },
      });
    });

    it('returns down when the fetch fails', async () => {
      const admin = kafkaCore.getKafka().admin();
      (admin.fetchOffsets as jest.Mock).mockRejectedValue(
        new Error('Fetch failed'),
      );

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

  describe('connections and shutdown', () => {
    it('checkBrokers uses the named connection admin', async () => {
      await healthIndicator.checkBrokers('b', 'analytics');
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(kafkaCore.getAdmin).toHaveBeenCalledWith('analytics');
    });

    it('checkConsumerLag uses the named connection admin', async () => {
      await healthIndicator.checkConsumerLag('l', 'g', 10, 'analytics');
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(kafkaCore.getAdmin).toHaveBeenCalledWith('analytics');
    });

    it('checkBrokers reports down after shutdown', async () => {
      kafkaCore.getAdmin.mockRejectedValue(
        new Error('Kafka connections are shut down'),
      );
      await expect(healthIndicator.checkBrokers('b')).resolves.toEqual({
        b: { status: 'down', error: 'Kafka connections are shut down' },
      });
    });
  });

  describe('with Terminus HealthCheckService', () => {
    const terminusCheck = async () => {
      const mod = await Test.createTestingModule({
        imports: [TerminusModule.forRoot({ logger: false })],
        providers: [
          KafkaHealthIndicator,
          { provide: KafkaCoreService, useValue: kafkaCore },
        ],
      }).compile();
      const health = mod.get(HealthCheckService);
      const indicator = mod.get(KafkaHealthIndicator);
      try {
        return await health.check([() => indicator.isHealthy('kafka')]);
      } finally {
        await mod.close();
      }
    };

    it('reports ok when ready', async () => {
      consumers = { 'default:g': 'running' };
      await expect(terminusCheck()).resolves.toMatchObject({
        status: 'ok',
        info: { kafka: { status: 'up' } },
      });
    });

    it('fails the check with the not-ready message', async () => {
      producers = { default: false };
      const error = await terminusCheck().catch((e: unknown) => e);
      expect(error).toBeInstanceOf(ServiceUnavailableException);
      expect(
        (error as ServiceUnavailableException).getResponse(),
      ).toMatchObject({
        status: 'error',
        error: {
          kafka: {
            status: 'down',
            message: 'Not ready: producer "default" not connected',
          },
        },
      });
    });
  });
});
