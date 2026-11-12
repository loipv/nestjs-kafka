import { Injectable, Optional } from '@nestjs/common';
import { KafkaCoreService } from '../services/kafka-core.service';

// Import types only, service is optional
import type {
  HealthIndicatorService,
  HealthIndicatorResult,
} from '@nestjs/terminus';

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
 *
 * `isHealthy` suits a readiness probe. librdkafka reconnects on its own, so
 * do not restart the app (liveness) on a Kafka outage; `checkBrokers` probes
 * broker reachability.
 */
@Injectable()
export class KafkaHealthIndicator {
  constructor(
    private readonly kafkaCore: KafkaCoreService,
    @Optional()
    private readonly healthIndicatorService?: HealthIndicatorService,
  ) {}

  /**
   * Up when every registered producer is connected and every consumer group
   * is running. Details list each producer and group; `message` names what is
   * not ready.
   */
  isHealthy(key: string): HealthIndicatorResult {
    const producers = Object.fromEntries(
      this.kafkaCore
        .getConnectionNames()
        .map((name) => [name, this.kafkaCore.isProducerConnected(name)]),
    );
    const consumers = this.kafkaCore.getConsumerStatuses();

    const problems = [
      ...Object.entries(producers)
        .filter(([, connected]) => !connected)
        .map(([name]) => `producer "${name}" not connected`),
      ...Object.entries(consumers)
        .filter(([, status]) => status !== 'running')
        .map(([group, status]) => `consumer group "${group}" is ${status}`),
    ];
    const details = { producers, consumers };

    if (problems.length === 0) {
      return this.healthIndicatorService
        ? this.healthIndicatorService.check(key).up(details)
        : { [key]: { status: 'up', ...details } };
    }

    const down = { ...details, message: `Not ready: ${problems.join('; ')}` };
    return this.healthIndicatorService
      ? this.healthIndicatorService.check(key).down(down)
      : { [key]: { status: 'down', ...down } };
  }

  /**
   * Check Kafka brokers connectivity and cluster info
   * Note: In confluent-kafka-javascript, describeCluster is not available.
   * We use listTopics to verify connectivity instead.
   */
  async checkBrokers(key: string): Promise<HealthIndicatorResult> {
    const admin = this.kafkaCore.getKafka().admin();
    try {
      await admin.connect();

      // Use listTopics to verify connectivity (describeCluster not available)
      const topics = await admin.listTopics();

      const details = {
        connected: true,
        topicCount: topics.length,
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
    } finally {
      try {
        await admin.disconnect();
      } catch {
        // Ignore disconnect errors
      }
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
    const admin = this.kafkaCore.getKafka().admin();
    try {
      await admin.connect();

      const committedOffsets = await admin.fetchOffsets({ groupId });

      let totalLag = 0;

      for (const topicOffset of committedOffsets) {
        try {
          // Fetch the latest (high-watermark) offsets for each topic
          const latestOffsets = await admin.fetchTopicOffsets(
            topicOffset.topic,
          );

          for (const partition of topicOffset.partitions) {
            const committed = parseInt(partition.offset, 10);
            if (Number.isNaN(committed)) continue;

            const latestPartition = latestOffsets.find(
              (o) => o.partition === partition.partition,
            );
            const latest = latestPartition
              ? parseInt(latestPartition.offset, 10)
              : committed;
            const lag = Number.isNaN(latest)
              ? 0
              : Math.max(0, latest - committed);
            totalLag += lag;
          }
        } catch {
          // Skip topics where latest offsets cannot be fetched
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
    } finally {
      try {
        await admin.disconnect();
      } catch {
        // Ignore disconnect errors
      }
    }
  }
}
