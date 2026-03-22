import {
  Injectable,
  Logger,
  forwardRef,
  Inject,
  OnApplicationShutdown,
} from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaClient } from './kafka-client.service';

type Consumer = KafkaJS.Consumer;
type EachMessagePayload = KafkaJS.EachMessagePayload;
type KafkaMessage = KafkaJS.KafkaMessage;
import { KafkaCoreService } from './kafka-core.service';
import {
  DlqRetryOptions,
  ConsumerMetadata,
  ConsumerOptions,
  DEFAULT_KAFKA_CONNECTION,
} from '../interfaces';
import { deserializeMessage } from '../interfaces/message.interface';
import { DlqMetricsService } from './dlq-metrics.service';

// DLQ Retry specific headers
export const DLQ_RETRY_HEADERS = {
  REPROCESS_COUNT: 'x-dlq-reprocess-count', // Renamed from x-dlq-consumer-retry-count
  REPROCESS_TIMESTAMP: 'x-dlq-reprocess-timestamp', // Renamed from x-dlq-retry-timestamp
  REPROCESS_ERROR: 'x-dlq-reprocess-error', // Renamed from x-dlq-retry-error
  // Final DLQ headers
  FINAL_DLQ_REASON: 'x-final-dlq-reason',
  FINAL_DLQ_REPROCESS_COUNT: 'x-final-dlq-reprocess-count',
  FINAL_DLQ_TIMESTAMP: 'x-final-dlq-timestamp',
  FINAL_DLQ_SOURCE: 'x-final-dlq-source',
} as const;

interface DlqTopicHandler {
  dlqTopic: string;
  retryOptions: DlqRetryOptions;
  originalTopic: string;
  originalHandler: (message: any) => Promise<void>;
  originalOptions: ConsumerOptions;
}

interface DlqConsumerGroup {
  groupId: string;
  connection: string;
  consumer: Consumer;
  topics: Map<string, DlqTopicHandler>;
  isRunning: boolean;
}

/**
 * Service that manages DLQ retry consumers.
 * Automatically consumes messages from DLQ topics, calls the original handler,
 * and on failure sends back to DLQ (with retry count).
 */
@Injectable()
export class DlqRetryService implements OnApplicationShutdown {
  private readonly logger = new Logger(DlqRetryService.name);
  private dlqConsumerGroups = new Map<string, DlqConsumerGroup>();
  private isShuttingDown = false;

  // Track original consumer groupIds to prevent collision
  private originalConsumerGroupIds = new Set<string>();

  constructor(
    private readonly kafkaCore: KafkaCoreService,
    @Inject(forwardRef(() => KafkaClient))
    private readonly kafkaClient: KafkaClient,
    private readonly metrics: DlqMetricsService,
  ) {}

  /**
   * Register an original consumer groupId to prevent DLQ groupId collision
   */
  registerOriginalGroupId(connection: string, groupId: string): void {
    this.originalConsumerGroupIds.add(`${connection}:${groupId}`);
  }

  /**
   * Register a DLQ retry consumer
   */
  registerDlqRetryConsumer(
    metadata: ConsumerMetadata,
    handler: (message: any) => Promise<void>,
  ): void {
    const { options, topic } = metadata;
    const dlqOptions = options.dlq;

    if (!dlqOptions?.retry?.enabled) {
      return;
    }

    const dlqTopic = dlqOptions.topic;
    const retryOptions = dlqOptions.retry;
    const connection = options.connection || DEFAULT_KAFKA_CONNECTION;

    // Generate groupId
    let groupId = retryOptions.groupId || `${dlqTopic}-retry-consumer`;

    // Check for collision with original consumer groupIds
    const groupKey = `${connection}:${groupId}`;
    if (this.originalConsumerGroupIds.has(groupKey)) {
      this.logger.warn(
        `DLQ retry groupId "${groupId}" collides with an original consumer groupId. Appending "-dlq" suffix.`,
      );
      groupId = `${groupId}-dlq`;
    }

    const consumerGroupKey = `${connection}:${groupId}`;

    // Get or create consumer group
    let group = this.dlqConsumerGroups.get(consumerGroupKey);
    if (!group) {
      const kafka = this.kafkaCore.getKafka(connection);
      const consumer = kafka.consumer({
        kafkaJS: {
          groupId,
          fromBeginning: retryOptions.fromBeginning ?? false,
        },
      });

      group = {
        groupId,
        connection,
        consumer,
        topics: new Map(),
        isRunning: false,
      };
      this.dlqConsumerGroups.set(consumerGroupKey, group);
    }

    // Register the DLQ topic handler
    group.topics.set(dlqTopic, {
      dlqTopic,
      retryOptions,
      originalTopic: topic,
      originalHandler: handler,
      originalOptions: options,
    });

    this.logger.log(
      `Registered DLQ retry for topic "${dlqTopic}" -> handler (group: ${groupId})`,
    );
  }

  /**
   * Start all registered DLQ consumer groups
   */
  async startAll(): Promise<void> {
    const startPromises = Array.from(this.dlqConsumerGroups.values()).map(
      (group) => this.startConsumerGroup(group),
    );
    await Promise.all(startPromises);
  }

  /**
   * Start a single DLQ consumer group
   */
  private async startConsumerGroup(group: DlqConsumerGroup): Promise<void> {
    if (group.isRunning) return;

    const { groupId, consumer, topics, connection } = group;

    try {
      await consumer.connect();

      // Subscribe to all DLQ topics in this group
      const topicList = Array.from(topics.keys());
      for (const dlqTopic of topicList) {
        await consumer.subscribe({ topic: dlqTopic });
        // Note: fromBeginning is configured at consumer level in confluent-kafka-javascript
      }

      this.logger.log(
        `DLQ consumer group "${groupId}" subscribed to topics: ${topicList.join(', ')}`,
      );

      await consumer.run({
        eachMessage: async (payload: EachMessagePayload) => {
          if (this.isShuttingDown) return;

          const { topic: dlqTopic, message, partition } = payload;

          const handler = topics.get(dlqTopic);
          if (!handler) {
            this.logger.warn(`No handler found for DLQ topic: ${dlqTopic}`);
            return;
          }

          await this.handleDlqMessage(
            message,
            partition,
            handler,
            connection,
            dlqTopic,
          );
        },
      });

      group.isRunning = true;
      this.logger.log(`DLQ consumer group "${groupId}" started`);
    } catch (error) {
      this.logger.error(
        `Failed to start DLQ consumer group: ${groupId}`,
        error,
      );
      throw error;
    }
  }

  /**
   * Handle a message from DLQ - call original handler, on failure send back to DLQ
   */
  private async handleDlqMessage(
    message: KafkaMessage,
    partition: number,
    handler: DlqTopicHandler,
    connection: string,
    dlqTopic: string,
  ): Promise<void> {
    const { retryOptions, originalHandler, originalOptions } = handler;
    const headers = message.headers || {};

    // Get current reprocess count (using new header name)
    const reprocessCountHeader = headers[DLQ_RETRY_HEADERS.REPROCESS_COUNT];
    const parsedCount = reprocessCountHeader
      ? parseInt(reprocessCountHeader.toString(), 10)
      : 0;
    const currentReprocessCount = Number.isNaN(parsedCount)
      ? 0
      : Math.max(0, parsedCount);

    const maxRetries = retryOptions.maxRetries ?? 3;
    const baseDelay = retryOptions.delay ?? 60000;
    const backoffMultiplier = retryOptions.backoffMultiplier ?? 2;

    // Record reprocess attempt
    this.metrics.recordReprocessAttempt(dlqTopic);

    // Check if we've exceeded max DLQ retries
    if (currentReprocessCount >= maxRetries) {
      await this.handleMaxRetriesExceeded(
        message,
        retryOptions,
        connection,
        dlqTopic,
        currentReprocessCount,
      );
      return;
    }

    // Calculate delay with exponential backoff
    const delay =
      baseDelay * Math.pow(backoffMultiplier, currentReprocessCount);

    this.logger.log(
      `DLQ reprocess ${currentReprocessCount + 1}/${maxRetries} for message from ${dlqTopic}, waiting ${delay}ms`,
    );

    // Wait before retrying
    await this.sleep(delay);

    try {
      // Deserialize message if needed (based on original consumer options)
      // Note: Use dlqTopic here so message.topic reflects the actual topic being consumed
      const processedMessage =
        originalOptions.deserialize !== false
          ? deserializeMessage(message, dlqTopic, partition)
          : message;

      // Call original handler
      await originalHandler(processedMessage);

      // Record success
      this.metrics.recordReprocessSuccess(dlqTopic);

      this.logger.log(
        `DLQ message processed successfully from ${dlqTopic} (reprocess ${currentReprocessCount + 1}/${maxRetries})`,
      );
    } catch (error) {
      this.logger.warn(
        `DLQ handler failed for ${dlqTopic}, sending back to DLQ (reprocess ${currentReprocessCount + 1}/${maxRetries})`,
      );

      // Build new headers with updated reprocess count
      const newHeaders: Record<string, string> = {};

      // Copy existing headers
      for (const [key, value] of Object.entries(headers)) {
        if (value !== undefined && value !== null) {
          newHeaders[key] = value.toString();
        }
      }

      // Use new header names
      newHeaders[DLQ_RETRY_HEADERS.REPROCESS_COUNT] = String(
        currentReprocessCount + 1,
      );
      newHeaders[DLQ_RETRY_HEADERS.REPROCESS_TIMESTAMP] =
        new Date().toISOString();
      newHeaders[DLQ_RETRY_HEADERS.REPROCESS_ERROR] = (error as Error).message;

      // Convert Buffer to string to maintain same format
      const messageValue = message.value
        ? message.value.toString('utf-8')
        : null;
      const messageKey = message.key
        ? Buffer.isBuffer(message.key)
          ? message.key.toString('utf-8')
          : message.key
        : null;

      await this.kafkaClient.send(
        dlqTopic,
        {
          key: messageKey,
          value: messageValue,
          headers: newHeaders,
        },
        { connection },
      );
    }
  }

  /**
   * Handle message that has exceeded max DLQ retries
   */
  private async handleMaxRetriesExceeded(
    message: KafkaMessage,
    retryOptions: DlqRetryOptions,
    connection: string,
    dlqTopic: string,
    reprocessCount: number,
  ): Promise<void> {
    const finalDlqTopic = retryOptions.finalDlqTopic;

    if (finalDlqTopic) {
      const headers: Record<string, string> = {};

      // Copy existing headers
      const existingHeaders = message.headers || {};
      for (const [key, value] of Object.entries(existingHeaders)) {
        if (value !== undefined && value !== null) {
          headers[key] = value.toString();
        }
      }

      // Use new header names
      headers[DLQ_RETRY_HEADERS.FINAL_DLQ_REASON] = 'max-reprocess-exceeded';
      headers[DLQ_RETRY_HEADERS.FINAL_DLQ_REPROCESS_COUNT] =
        String(reprocessCount);
      headers[DLQ_RETRY_HEADERS.FINAL_DLQ_TIMESTAMP] = new Date().toISOString();
      headers[DLQ_RETRY_HEADERS.FINAL_DLQ_SOURCE] = dlqTopic;

      try {
        const messageValue = message.value
          ? message.value.toString('utf-8')
          : null;
        const messageKey = message.key
          ? Buffer.isBuffer(message.key)
            ? message.key.toString('utf-8')
            : message.key
          : null;

        await this.kafkaClient.send(
          finalDlqTopic,
          {
            key: messageKey,
            value: messageValue,
            headers,
          },
          { connection },
        );

        // Record as sent to final DLQ
        this.metrics.recordFinalFailure(dlqTopic, true);

        this.logger.warn(
          `Message sent to final DLQ ${finalDlqTopic} after ${reprocessCount} reprocesses from ${dlqTopic}`,
        );
      } catch (error) {
        this.logger.error(
          `Failed to send message to final DLQ ${finalDlqTopic}`,
          error,
        );
      }
    } else {
      // No finalDlqTopic configured - drop message after max retries
      this.metrics.recordFinalFailure(dlqTopic, false);
      this.logger.warn(
        `Message dropped after ${reprocessCount} DLQ reprocesses from ${dlqTopic} (no finalDlqTopic configured)`,
      );
    }
  }

  private shutdownResolvers = new Set<() => void>();

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.shutdownResolvers.delete(cancelFn);
        resolve();
      }, ms);

      const cancelFn = () => {
        clearTimeout(timer);
        resolve();
      };

      this.shutdownResolvers.add(cancelFn);
    });
  }

  async gracefulShutdown(): Promise<void> {
    this.isShuttingDown = true;

    // Cancel all in-flight retry delays so shutdown isn't blocked
    for (const cancel of this.shutdownResolvers) {
      cancel();
    }
    this.shutdownResolvers.clear();

    this.logger.log('Gracefully shutting down DLQ retry consumers...');

    const shutdownPromises = Array.from(this.dlqConsumerGroups.values()).map(
      async (group) => {
        if (group.isRunning) {
          try {
            await group.consumer.disconnect();
            group.isRunning = false;
            this.logger.log(
              `DLQ consumer group "${group.groupId}" disconnected`,
            );
          } catch (error) {
            this.logger.error(
              `Error disconnecting DLQ consumer group "${group.groupId}"`,
              error,
            );
          }
        }
      },
    );

    await Promise.all(shutdownPromises);
  }

  async onApplicationShutdown(): Promise<void> {
    await this.gracefulShutdown();
  }
}
