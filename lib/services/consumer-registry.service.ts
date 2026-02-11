/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  Injectable,
  Inject,
  Logger,
  OnApplicationShutdown,
  Optional,
} from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaCoreService } from './kafka-core.service';

type Consumer = KafkaJS.Consumer;
type EachBatchPayload = KafkaJS.EachBatchPayload;
type EachMessagePayload = KafkaJS.EachMessagePayload;
import { BatchProcessorService } from './batch-processor.service';
import { IdempotencyService } from './idempotency.service';
import { PressureManagerService } from './pressure-manager.service';
import { DlqService } from './dlq.service';
import { DlqRetryService } from './dlq-retry.service';
import { TracingService } from './tracing.service';
import {
  ConsumerMetadata,
  ConsumerModuleOptions,
  ConsumerOptions,
  CONSUMER_MODULE_OPTIONS,
  DEFAULT_KAFKA_CONNECTION,
  deserializeMessage,
} from '../interfaces';

interface TopicHandler {
  metadata: ConsumerMetadata;
  handler: (...args: any[]) => Promise<void>;
}

interface ConsumerGroup {
  groupId: string;
  connection: string;
  consumer: Consumer;
  topics: Map<string, TopicHandler>;
  options: ConsumerOptions; // Use first consumer's options for shared settings
  isRunning: boolean;
  hasBatchConsumer: boolean;
}

interface MessageRetryState {
  retryCount: number;
  lastError?: Error;
}

@Injectable()
export class ConsumerRegistryService implements OnApplicationShutdown {
  private readonly logger = new Logger(ConsumerRegistryService.name);
  private consumerGroups = new Map<string, ConsumerGroup>();
  private isShuttingDown = false;
  private messageRetryStates = new Map<string, MessageRetryState>();

  constructor(
    private readonly kafkaCore: KafkaCoreService,
    private readonly batchProcessor: BatchProcessorService,
    private readonly idempotencyService: IdempotencyService,
    private readonly pressureManager: PressureManagerService,
    private readonly dlqService: DlqService,
    private readonly dlqRetryService: DlqRetryService,
    @Optional() private readonly tracingService?: TracingService,
    @Optional()
    @Inject(CONSUMER_MODULE_OPTIONS)
    private readonly moduleOptions?: ConsumerModuleOptions,
  ) {}

  /**
   * Merge module default options with decorator options.
   * Decorator options take precedence over module defaults.
   */
  private mergeWithDefaults(options: ConsumerOptions): ConsumerOptions {
    if (!this.moduleOptions) {
      return options;
    }

    const defaults = this.moduleOptions;

    return {
      // Module defaults (applied if decorator doesn't specify)
      ...(defaults.partitionAssigners !== undefined &&
        options.partitionAssigners === undefined && {
          partitionAssigners: defaults.partitionAssigners,
        }),
      ...(defaults.allowAutoTopicCreation !== undefined &&
        options.allowAutoTopicCreation === undefined && {
          allowAutoTopicCreation: defaults.allowAutoTopicCreation,
        }),
      ...(defaults.sessionTimeout !== undefined &&
        options.sessionTimeout === undefined && {
          sessionTimeout: defaults.sessionTimeout,
        }),
      ...(defaults.heartbeatInterval !== undefined &&
        options.heartbeatInterval === undefined && {
          heartbeatInterval: defaults.heartbeatInterval,
        }),
      ...(defaults.rebalanceTimeout !== undefined &&
        options.rebalanceTimeout === undefined && {
          rebalanceTimeout: defaults.rebalanceTimeout,
        }),
      ...(defaults.autoCommit !== undefined &&
        options.autoCommit === undefined && {
          autoCommit: defaults.autoCommit,
        }),
      ...(defaults.autoCommitInterval !== undefined &&
        options.autoCommitInterval === undefined && {
          autoCommitInterval: defaults.autoCommitInterval,
        }),
      ...(defaults.fromBeginning !== undefined &&
        options.fromBeginning === undefined && {
          fromBeginning: defaults.fromBeginning,
        }),
      // Merge retry options (decorator retry options take precedence)
      ...(defaults.retry &&
        !options.retry && {
          retry: defaults.retry,
        }),
      // Decorator options (always applied, overrides defaults)
      ...options,
    };
  }

  registerConsumers(consumers: ConsumerMetadata[]): void {
    // Group consumers by groupId
    for (const metadata of consumers) {
      this.registerConsumer(metadata);
    }
  }

  private registerConsumer(metadata: ConsumerMetadata): void {
    const { topic, connection, target, methodName } = metadata;
    // Merge module defaults with decorator options
    const options = this.mergeWithDefaults(metadata.options);
    const groupId = options.groupId || `${topic}-group`;
    const connectionName = connection || DEFAULT_KAFKA_CONNECTION;

    // Key includes both connection and groupId to support same groupId on different connections
    const groupKey = `${connectionName}:${groupId}`;

    // Get or create consumer group
    let group = this.consumerGroups.get(groupKey);

    if (!group) {
      // Build consumer config with only defined values
      const consumerConfig: KafkaJS.ConsumerConfig = {
        groupId,
        maxBytesPerPartition: 1048576,
        autoCommit: options.autoCommit !== false,
      };

      // Add optional settings only if defined
      if (options.sessionTimeout !== undefined) {
        consumerConfig.sessionTimeout = options.sessionTimeout;
      }
      if (options.heartbeatInterval !== undefined) {
        consumerConfig.heartbeatInterval = options.heartbeatInterval;
      }
      if (options.rebalanceTimeout !== undefined) {
        consumerConfig.rebalanceTimeout = options.rebalanceTimeout;
      }
      if (options.fromBeginning !== undefined) {
        consumerConfig.fromBeginning = options.fromBeginning;
      }
      if (options.autoCommitInterval !== undefined) {
        consumerConfig.autoCommitInterval = options.autoCommitInterval;
      }
      if (options.allowAutoTopicCreation !== undefined) {
        consumerConfig.allowAutoTopicCreation = options.allowAutoTopicCreation;
      }
      if (options.partitionAssigners && options.partitionAssigners.length > 0) {
        consumerConfig.partitionAssigners =
          options.partitionAssigners as KafkaJS.PartitionAssigners[];
      }
      if (options.retry) {
        consumerConfig.retry = {
          retries: options.retry.retries,
          maxRetryTime: options.retry.maxRetryTime,
          initialRetryTime: options.retry.initialRetryTime,
        };
      }

      const consumer = this.kafkaCore
        .getKafka(connectionName)
        .consumer({ kafkaJS: consumerConfig });

      group = {
        groupId,
        connection: connectionName,
        consumer,
        topics: new Map(),
        options,
        isRunning: false,
        hasBatchConsumer: false,
      };

      this.consumerGroups.set(groupKey, group);

      // Register this groupId with DLQ retry service to prevent collision
      this.dlqRetryService.registerOriginalGroupId(connectionName, groupId);

      this.pressureManager.register(groupKey, consumer, {
        backPressureThreshold: options.backPressureThreshold || 80,
        resumeThreshold: 60,
        maxQueueSize: options.maxQueueSize || 1000,
        checkIntervalMs: 1000,
      });

      this.logger.log(
        `Created consumer group: ${groupId} (connection: ${connectionName})`,
      );
    }

    // Check if mixing batch and non-batch consumers in same group
    if (options.batch) {
      group.hasBatchConsumer = true;
    }

    // Add topic handler to the group with merged options
    const handler = target[methodName].bind(target);
    const mergedMetadata: ConsumerMetadata = {
      ...metadata,
      options, // Use merged options
    };
    group.topics.set(topic, { metadata: mergedMetadata, handler });

    this.logger.log(
      `Registered topic "${topic}" in group "${groupId}" (connection: ${connectionName})`,
    );

    // Register DLQ retry consumer if enabled
    if (options.dlq?.retry?.enabled) {
      try {
        this.dlqRetryService.registerDlqRetryConsumer(mergedMetadata, handler);
      } catch (err) {
        this.logger.error(
          `Failed to register DLQ retry consumer for ${topic}`,
          err,
        );
      }
    }
  }

  async startAll(): Promise<void> {
    const startPromises = Array.from(this.consumerGroups.values()).map(
      (group) => this.startConsumerGroup(group),
    );

    await Promise.all(startPromises);

    // Start DLQ retry consumers
    await this.dlqRetryService.startAll();
  }

  private async startConsumerGroup(group: ConsumerGroup): Promise<void> {
    const { groupId, consumer, topics } = group;

    try {
      await consumer.connect();

      // Subscribe to all topics in this group
      const topicList = Array.from(topics.keys());

      // Check if any topic needs auto-creation (including DLQ topics)
      const topicsToCreate: Set<string> = new Set();
      for (const topic of topicList) {
        const topicHandler = topics.get(topic)!;
        const opts = topicHandler.metadata.options;

        if (opts.allowAutoTopicCreation) {
          topicsToCreate.add(topic);

          // Also auto-create DLQ topic if configured
          if (opts.dlq?.topic) {
            topicsToCreate.add(opts.dlq.topic);
          }

          // Also auto-create final DLQ topic if configured
          if (opts.dlq?.retry?.finalDlqTopic) {
            topicsToCreate.add(opts.dlq.retry.finalDlqTopic);
          }
        }
      }

      // Auto-create topics if needed
      if (topicsToCreate.size > 0) {
        const admin = this.kafkaCore.getKafka(group.connection).admin();
        try {
          await admin.connect();
          const existingTopics = await admin.listTopics();
          const newTopics = Array.from(topicsToCreate).filter(
            (t) => !existingTopics.includes(t),
          );

          if (newTopics.length > 0) {
            await admin.createTopics({
              topics: newTopics.map((topic) => ({
                topic,
                numPartitions: 1,
                replicationFactor: 1,
              })),
            });
            this.logger.log(`Auto-created topics: ${newTopics.join(', ')}`);
          }
        } catch (error) {
          this.logger.warn(`Failed to auto-create topics: ${error}`);
        } finally {
          await admin.disconnect();
        }
      }

      for (const topic of topicList) {
        await consumer.subscribe({ topic });
        // Note: fromBeginning is configured at consumer level in confluent-kafka-javascript
      }

      this.logger.log(
        `Consumer group "${groupId}" subscribed to topics: ${topicList.join(', ')}`,
      );

      // Determine if we should use batch or message processing
      // If any consumer in the group uses batch, we need special handling
      if (group.hasBatchConsumer) {
        await this.startBatchGroupConsumer(group);
      } else {
        await this.startMessageGroupConsumer(group);
      }

      group.isRunning = true;
      this.logger.log(`Started consumer group: ${groupId}`);
    } catch (error) {
      this.logger.error(`Failed to start consumer group: ${groupId}`, error);
      throw error;
    }
  }

  private async startMessageGroupConsumer(group: ConsumerGroup): Promise<void> {
    const { groupId, consumer, topics, options } = group;

    // Build run config with only defined values
    // Note: restartOnFailure is not supported in confluent-kafka-javascript
    const runConfig: KafkaJS.ConsumerRunConfig = {};

    if (
      options.partitionsConsumedConcurrently !== undefined &&
      options.partitionsConsumedConcurrently > 0
    ) {
      runConfig.partitionsConsumedConcurrently =
        options.partitionsConsumedConcurrently;
    }

    runConfig.eachMessage = async (payload: EachMessagePayload) => {
      if (this.isShuttingDown) return;

      const { topic, message, partition } = payload;

      // Find the handler for this topic
      const topicHandler = topics.get(topic);
      if (!topicHandler) {
        this.logger.warn(`No handler found for topic: ${topic}`);
        return;
      }

      const { metadata, handler } = topicHandler;
      const topicOptions = metadata.options;

      if (topicOptions.idempotencyKey) {
        if (
          this.idempotencyService.isProcessed(
            message,
            topicOptions.idempotencyKey,
          )
        ) {
          this.logger.debug(`Skipping duplicate message from ${topic}`);
          return;
        }
      }

      // Wrap message processing in a trace span
      const processMessage = async () => {
        const processedMessage =
          topicOptions.deserialize !== false
            ? deserializeMessage(message, topic, partition)
            : message;

        await handler(processedMessage);

        if (topicOptions.idempotencyKey) {
          this.idempotencyService.markProcessed(
            message,
            topicOptions.idempotencyKey,
          );
        }

        // Clear retry states for both DLQ and non-DLQ
        this.dlqService.clearRetryState(message, topic, partition);
        this.clearMessageRetryState(message, topic, partition);
      };

      try {
        if (this.tracingService?.isEnabled()) {
          await this.tracingService.withConsumeSpan(
            {
              topic,
              partition,
              offset: message.offset,
              key: message.key?.toString(),
              groupId,
              headers: message.headers,
            },
            processMessage,
          );
        } else {
          await processMessage();
        }
      } catch (error) {
        await this.handleError(message, error as Error, metadata, partition);
      }
    };

    await consumer.run(runConfig);
  }

  private async startBatchGroupConsumer(group: ConsumerGroup): Promise<void> {
    const { groupId, consumer, topics, options } = group;

    // Build run config with only defined values
    // Note: restartOnFailure is not supported in confluent-kafka-javascript
    const runConfig: KafkaJS.ConsumerRunConfig = {};

    if (
      options.partitionsConsumedConcurrently !== undefined &&
      options.partitionsConsumedConcurrently > 0
    ) {
      runConfig.partitionsConsumedConcurrently =
        options.partitionsConsumedConcurrently;
    }

    runConfig.eachBatch = async (payload: EachBatchPayload) => {
      if (this.isShuttingDown) return;

      const { batch } = payload;
      const { topic, partition, messages } = batch;

      // Find the handler for this topic
      const topicHandler = topics.get(topic);
      if (!topicHandler) {
        this.logger.warn(`No handler found for topic: ${topic}`);
        return;
      }

      const { metadata, handler } = topicHandler;
      const topicOptions = metadata.options;

      // Check if this topic uses batch processing
      if (topicOptions.batch) {
        // Use batch accumulator
        const accumulator =
          this.batchProcessor.createBatchAccumulator(topicOptions);

        accumulator.onFlush(async (batchMessages) => {
          await this.processBatchMessages(
            batchMessages,
            topic,
            partition,
            topicOptions,
            handler,
            groupId,
          );
        });

        for (const message of messages) {
          if (!payload.isRunning() || payload.isStale()) break;

          await accumulator.add(message);
          payload.resolveOffset(message.offset);
          // Note: heartbeat() is automatic in confluent-kafka-javascript
        }

        await accumulator.flush();
      } else {
        // Process messages one by one (non-batch consumer in a batch group)
        for (const message of messages) {
          if (!payload.isRunning() || payload.isStale()) break;

          if (topicOptions.idempotencyKey) {
            if (
              this.idempotencyService.isProcessed(
                message,
                topicOptions.idempotencyKey,
              )
            ) {
              payload.resolveOffset(message.offset);
              // Note: heartbeat() is automatic in confluent-kafka-javascript
              continue;
            }
          }

          // Wrap message processing in a trace span
          const processMessage = async () => {
            const processedMessage =
              topicOptions.deserialize !== false
                ? deserializeMessage(message, topic, partition)
                : message;

            await handler(processedMessage);

            if (topicOptions.idempotencyKey) {
              this.idempotencyService.markProcessed(
                message,
                topicOptions.idempotencyKey,
              );
            }

            // Clear retry states for both DLQ and non-DLQ
            this.dlqService.clearRetryState(message, topic, partition);
            this.clearMessageRetryState(message, topic, partition);
          };

          try {
            if (this.tracingService?.isEnabled()) {
              await this.tracingService.withConsumeSpan(
                {
                  topic,
                  partition,
                  offset: message.offset,
                  key: message.key?.toString(),
                  groupId,
                  headers: message.headers,
                },
                processMessage,
              );
            } else {
              await processMessage();
            }
          } catch (error) {
            await this.handleError(
              message,
              error as Error,
              metadata,
              partition,
            );
          }

          payload.resolveOffset(message.offset);
          // Note: heartbeat() is automatic in confluent-kafka-javascript
        }
      }
    };

    await consumer.run(runConfig);
  }

  private async processBatchMessages(
    messages: any[],
    topic: string,
    partition: number,
    options: ConsumerOptions,
    handler: (...args: any[]) => Promise<void>,
    groupId?: string,
  ): Promise<void> {
    let processableMessages = messages;

    if (options.idempotencyKey) {
      processableMessages = this.idempotencyService.filterDuplicates(
        messages,
        options.idempotencyKey,
      );
    }

    const deserializedMessages =
      options.deserialize !== false
        ? processableMessages.map((msg) =>
            deserializeMessage(msg, topic, partition),
          )
        : processableMessages;

    // Process batch - create a span for the batch if tracing is enabled
    const processBatch = async () => {
      if (options.groupByKey) {
        const grouped =
          this.batchProcessor.groupMessagesByKey(deserializedMessages);
        await handler(grouped);
      } else {
        await handler(deserializedMessages);
      }
    };

    // For batch processing, use links to connect all message traces
    // First message becomes parent, others are linked
    if (this.tracingService?.isEnabled() && messages.length > 0) {
      await this.tracingService.withBatchConsumeSpan(
        {
          topic,
          partition,
          groupId,
          messagesHeaders: messages.map((msg) => ({
            offset: msg.offset,
            key: msg.key?.toString(),
            headers: msg.headers,
          })),
        },
        processBatch,
      );
    } else {
      await processBatch();
    }

    if (options.idempotencyKey) {
      for (const msg of processableMessages) {
        this.idempotencyService.markProcessed(msg, options.idempotencyKey);
      }
    }
  }

  private async handleError(
    message: any,
    error: Error,
    metadata: ConsumerMetadata,
    partition?: number,
  ): Promise<void> {
    const { topic, options, connection } = metadata;

    this.logger.error(`Error processing message from ${topic}`, error);

    if (options.dlq) {
      // With DLQ: Use DLQ service for retry and DLQ handling
      const shouldRetry = await this.dlqService.handleFailure(
        message,
        error,
        options.dlq,
        topic,
        partition,
        connection,
      );

      if (shouldRetry) {
        throw error;
      }
    } else {
      // Without DLQ: Implement retry mechanism here
      const messageKey = this.getMessageKey(message, topic, partition);
      let state = this.messageRetryStates.get(messageKey);

      if (!state) {
        state = { retryCount: 0 };
        this.messageRetryStates.set(messageKey, state);
      }

      state.retryCount++;
      state.lastError = error;

      // Get max retries from retry options, default to 3
      const maxRetries = options.retry?.retries ?? 3;

      if (state.retryCount <= maxRetries) {
        // Calculate exponential backoff delay
        const baseDelay = options.retry?.initialRetryTime ?? 1000;
        const multiplier = options.retry?.multiplier ?? 2;
        const delay = baseDelay * Math.pow(multiplier, state.retryCount - 1);

        this.logger.warn(
          `Retry ${state.retryCount}/${maxRetries} for message from ${topic}, waiting ${delay}ms`,
        );

        await this.sleep(delay);
        throw error; // Retry: throw error to trigger consumer retry
      } else {
        // Exceeded max retries
        const skipMessage = options.retry?.skipMessageOnMaxRetries ?? false;

        this.messageRetryStates.delete(messageKey);

        if (skipMessage) {
          // Skip message to avoid blocking consumer
          this.logger.error(
            `Message from ${topic} failed after ${state.retryCount} retries. Skipping message to avoid blocking consumer. ` +
              `Offset: ${message.offset}, Partition: ${partition ?? 'unknown'}`,
          );
          this.logger.error(
            `Dropped message details - Topic: ${topic}, Key: ${message.key?.toString()}, Error: ${error.message}`,
          );
          // DO NOT throw error - let consumer commit offset and skip this message
          // This allows consumer to continue processing other messages
        } else {
          // Throw error to potentially restart consumer
          this.logger.error(
            `Message from ${topic} failed after ${state.retryCount} retries. Throwing error as configured. ` +
              `Offset: ${message.offset}, Partition: ${partition ?? 'unknown'}`,
          );
          throw error;
        }
      }
    }
  }

  private getMessageKey(
    message: any,
    topic: string,
    partition?: number,
  ): string {
    return `${topic}:${partition ?? 0}:${message.offset}`;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private clearMessageRetryState(
    message: any,
    topic: string,
    partition?: number,
  ): void {
    const key = this.getMessageKey(message, topic, partition);
    this.messageRetryStates.delete(key);
  }

  async gracefulShutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.logger.log('Starting graceful shutdown of consumers...');

    const shutdownPromises = Array.from(this.consumerGroups.values()).map(
      async (group) => {
        try {
          if (group.isRunning) {
            // Note: stop() is not supported in confluent-kafka-javascript, use disconnect() directly
            await group.consumer.disconnect();
          }
        } catch (error) {
          this.logger.error(
            `Error during consumer group shutdown: ${group.groupId}`,
            error,
          );
        }
      },
    );

    await Promise.all(shutdownPromises);
    this.idempotencyService.stopCleanup();

    // Shutdown DLQ retry consumers
    await this.dlqRetryService.gracefulShutdown();

    // Clear retry states
    this.messageRetryStates.clear();

    this.logger.log('All consumer groups shut down gracefully');
  }

  async onApplicationShutdown(): Promise<void> {
    await this.gracefulShutdown();
  }
}
