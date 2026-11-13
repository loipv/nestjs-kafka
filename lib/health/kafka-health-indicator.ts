import { Injectable } from '@nestjs/common';
import { KafkaCoreService } from '../services/kafka-core.service';

// Type only: @nestjs/terminus is an optional peer dependency
import type { HealthIndicatorResult } from '@nestjs/terminus';

/**
 * Kafka Health Indicator for @nestjs/terminus
 *
 * Returns plain `HealthIndicatorResult` objects (`{ [key]: { status, ... } }`);
 * Terminus' HealthCheckService reports a `down` result as a failed check.
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
  constructor(private readonly kafkaCore: KafkaCoreService) {}

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

    // A producer whose connect failed is only retried by the next send();
    // a readiness probe must not wait for traffic that cannot arrive.
    for (const [name, connected] of Object.entries(producers)) {
      if (!connected) this.kafkaCore.connectProducerInBackground(name);
    }

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
      return { [key]: { status: 'up', ...details } };
    }
    return {
      [key]: {
        status: 'down',
        ...details,
        message: `Not ready: ${problems.join('; ')}`,
      },
    };
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

      return { [key]: { status: 'up', ...details } };
    } catch (error) {
      const errorDetails = { error: (error as Error).message };

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

      return { [key]: { status: isHealthy ? 'up' : 'down', ...details } };
    } catch (error) {
      const errorDetails = { error: (error as Error).message };

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
