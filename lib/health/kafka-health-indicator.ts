import { Injectable, Optional } from '@nestjs/common';
import { KafkaClient } from '../services/kafka-client.service';
import { KafkaCoreService } from '../services/kafka-core.service';

// Import types only, service is optional
import type { HealthIndicatorService, HealthIndicatorResult } from '@nestjs/terminus';

/**
 * Kafka Health Indicator for @nestjs/terminus
 * 
 * Note: To use this health indicator, you must import TerminusModule in your application.
 * If TerminusModule is not imported, the health indicator will use a fallback implementation.
 *
 * @example
 * ```typescript
 * // app.module.ts
 * import { TerminusModule } from '@nestjs/terminus';
 * 
 * @Module({
 *   imports: [
 *     TerminusModule,
 *     KafkaModule.forRoot({ ... }),
 *   ],
 * })
 * export class AppModule {}
 * 
 * // health.controller.ts
 * @Controller('health')
 * export class HealthController {
 *   constructor(
 *     private health: HealthCheckService,
 *     private kafkaHealth: KafkaHealthIndicator,
 *   ) {}
 *
 *   @Get()
 *   @HealthCheck()
 *   check() {
 *     return this.health.check([
 *       () => this.kafkaHealth.isHealthy('kafka'),
 *     ]);
 *   }
 * }
 * ```
 */
@Injectable()
export class KafkaHealthIndicator {
  constructor(
    private readonly kafkaClient: KafkaClient,
    private readonly kafkaCore: KafkaCoreService,
    @Optional() private readonly healthIndicatorService?: HealthIndicatorService,
  ) { }

  /**
   * Check if Kafka producer is healthy (connected)
   */
  isHealthy(key: string): HealthIndicatorResult {
    const isHealthy = this.kafkaClient.isHealthy();

    if (this.healthIndicatorService) {
      const indicator = this.healthIndicatorService.check(key);
      if (isHealthy) {
        return indicator.up({ connected: true });
      }
      return indicator.down({ connected: false, message: 'Kafka producer is not connected' });
    }

    // Fallback without TerminusModule
    return {
      [key]: {
        status: isHealthy ? 'up' : 'down',
        connected: isHealthy,
        ...(isHealthy ? {} : { message: 'Kafka producer is not connected' }),
      },
    };
  }

  /**
   * Check Kafka brokers connectivity and cluster info
   */
  async checkBrokers(key: string): Promise<HealthIndicatorResult> {
    try {
      const admin = this.kafkaCore.getKafka().admin();
      await admin.connect();

      const clusterInfo = await admin.describeCluster();
      await admin.disconnect();

      const details = {
        brokers: clusterInfo.brokers.length,
        controller: clusterInfo.controller,
        clusterId: clusterInfo.clusterId,
      };

      if (this.healthIndicatorService) {
        return this.healthIndicatorService.check(key).up(details);
      }

      return { [key]: { status: 'up', ...details } };
    } catch (error) {
      const errorDetails = { error: (error as Error).message };

      if (this.healthIndicatorService) {
        return this.healthIndicatorService.check(key).down(errorDetails);
      }

      return { [key]: { status: 'down', ...errorDetails } };
    }
  }

  /**
   * Check consumer lag for a specific consumer group
   */
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
      const details = { groupId, lag: totalLag, maxLag };

      if (this.healthIndicatorService) {
        const indicator = this.healthIndicatorService.check(key);
        return isHealthy ? indicator.up(details) : indicator.down(details);
      }

      return { [key]: { status: isHealthy ? 'up' : 'down', ...details } };
    } catch (error) {
      const errorDetails = { error: (error as Error).message };

      if (this.healthIndicatorService) {
        return this.healthIndicatorService.check(key).down(errorDetails);
      }

      return { [key]: { status: 'down', ...errorDetails } };
    }
  }
}
