import { Injectable } from '@nestjs/common';
import {
  HealthIndicatorService,
  HealthIndicatorResult,
} from '@nestjs/terminus';
import { KafkaClient } from '../services/kafka-client.service';
import { KafkaCoreService } from '../services/kafka-core.service';

@Injectable()
export class KafkaHealthIndicator {
  constructor(
    private readonly healthIndicatorService: HealthIndicatorService,
    private readonly kafkaClient: KafkaClient,
    private readonly kafkaCore: KafkaCoreService,
  ) {}

  isHealthy(key: string): HealthIndicatorResult {
    const indicator = this.healthIndicatorService.check(key);
    const isHealthy = this.kafkaClient.isHealthy();

    if (isHealthy) {
      return indicator.up({ connected: true });
    }

    return indicator.down({
      connected: false,
      message: 'Kafka producer is not connected',
    });
  }

  async checkBrokers(key: string): Promise<HealthIndicatorResult> {
    const indicator = this.healthIndicatorService.check(key);

    try {
      const admin = this.kafkaCore.getKafka().admin();
      await admin.connect();

      const clusterInfo = await admin.describeCluster();
      await admin.disconnect();

      return indicator.up({
        brokers: clusterInfo.brokers.length,
        controller: clusterInfo.controller,
        clusterId: clusterInfo.clusterId,
      });
    } catch (error) {
      return indicator.down({
        error: (error as Error).message,
      });
    }
  }

  async checkConsumerLag(
    key: string,
    groupId: string,
    maxLag: number = 1000,
  ): Promise<HealthIndicatorResult> {
    const indicator = this.healthIndicatorService.check(key);

    try {
      const admin = this.kafkaCore.getKafka().admin();
      await admin.connect();

      const offsets = await admin.fetchOffsets({ groupId });
      await admin.disconnect();

      let totalLag = 0;
      for (const topicOffset of offsets) {
        for (const partition of topicOffset.partitions) {
          const offset = parseInt(partition.offset, 10);
          totalLag += Math.max(0, offset);
        }
      }

      const isHealthy = totalLag < maxLag;

      if (isHealthy) {
        return indicator.up({
          groupId,
          lag: totalLag,
          maxLag,
        });
      }

      return indicator.down({
        groupId,
        lag: totalLag,
        maxLag,
      });
    } catch (error) {
      return indicator.down({
        error: (error as Error).message,
      });
    }
  }
}
