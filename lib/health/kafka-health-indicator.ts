import { Injectable, Optional } from '@nestjs/common';
import { KafkaClient } from '../services/kafka-client.service';
import { KafkaCoreService } from '../services/kafka-core.service';

// Try to import from terminus, but make it optional
let HealthIndicatorService: any;
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const terminus = require('@nestjs/terminus');
  HealthIndicatorService = terminus.HealthIndicatorService;
} catch {
  HealthIndicatorService = null;
}

export interface HealthIndicatorResult {
  [key: string]: {
    status: string;
    [key: string]: any;
  };
}

@Injectable()
export class KafkaHealthIndicator {
  constructor(
    private readonly kafkaClient: KafkaClient,
    private readonly kafkaCore: KafkaCoreService,
    @Optional() private readonly healthIndicatorService?: any,
  ) { }

  isHealthy(key: string): HealthIndicatorResult {
    const isHealthy = this.kafkaClient.isHealthy();

    if (this.healthIndicatorService) {
      const indicator = this.healthIndicatorService.check(key);
      if (isHealthy) {
        return indicator.up({ connected: true });
      }
      return indicator.down({
        connected: false,
        message: 'Kafka producer is not connected',
      });
    }

    // Fallback without terminus
    return {
      [key]: {
        status: isHealthy ? 'up' : 'down',
        connected: isHealthy,
        ...(isHealthy ? {} : { message: 'Kafka producer is not connected' }),
      },
    };
  }

  async checkBrokers(key: string): Promise<HealthIndicatorResult> {
    try {
      const admin = this.kafkaCore.getKafka().admin();
      await admin.connect();

      const clusterInfo = await admin.describeCluster();
      await admin.disconnect();

      const result = {
        brokers: clusterInfo.brokers.length,
        controller: clusterInfo.controller,
        clusterId: clusterInfo.clusterId,
      };

      if (this.healthIndicatorService) {
        return this.healthIndicatorService.check(key).up(result);
      }

      return {
        [key]: {
          status: 'up',
          ...result,
        },
      };
    } catch (error) {
      const errorResult = { error: (error as Error).message };

      if (this.healthIndicatorService) {
        return this.healthIndicatorService.check(key).down(errorResult);
      }

      return {
        [key]: {
          status: 'down',
          ...errorResult,
        },
      };
    }
  }

  async checkConsumerLag(
    key: string,
    groupId: string,
    maxLag: number = 1000,
  ): Promise<HealthIndicatorResult> {
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
      const result = { groupId, lag: totalLag, maxLag };

      if (this.healthIndicatorService) {
        const indicator = this.healthIndicatorService.check(key);
        return isHealthy ? indicator.up(result) : indicator.down(result);
      }

      return {
        [key]: {
          status: isHealthy ? 'up' : 'down',
          ...result,
        },
      };
    } catch (error) {
      const errorResult = { error: (error as Error).message };

      if (this.healthIndicatorService) {
        return this.healthIndicatorService.check(key).down(errorResult);
      }

      return {
        [key]: {
          status: 'down',
          ...errorResult,
        },
      };
    }
  }
}
