/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common';
import { Consumer, EachBatchPayload, EachMessagePayload } from 'kafkajs';
import { KafkaCoreService } from './kafka-core.service';
import { BatchProcessorService } from './batch-processor.service';
import { IdempotencyService } from './idempotency.service';
import { PressureManagerService } from './pressure-manager.service';
import { DlqService } from './dlq.service';
import { DlqRetryService } from './dlq-retry.service';
import {
  ConsumerMetadata,
  ConsumerOptions,
  ConsumerRetryOptions,
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

@Injectable()
export class ConsumerRegistryService implements OnApplicationShutdown {
  private readonly logger = new Logger(ConsumerRegistryService.name);
  private consumerGroups = new Map<string, ConsumerGroup>();
  private isShuttingDown = false;

  constructor(
    private readonly kafkaCore: KafkaCoreService,
    private readonly batchProcessor: BatchProcessorService,
    private readonly idempotencyService: IdempotencyService,
    private readonly pressureManager: PressureManagerService,
    private readonly dlqService: DlqService,
    private readonly dlqRetryService: DlqRetryService,
  ) { }

  registerConsumers(consumers: ConsumerMetadata[]): void {
    // Group consumers by groupId
    for (const metadata of consumers) {
      this.registerConsumer(metadata);
    }
  }

  private registerConsumer(metadata: ConsumerMetadata): void {
    const { topic, connection, options, target, methodName } = metadata;
    const groupId = options.groupId || `${topic}-group`;
    const connectionName = connection || DEFAULT_KAFKA_CONNECTION;

    // Key includes both connection and groupId to support same groupId on different connections
    const groupKey = `${connectionName}:${groupId}`;

    // Get or create consumer group
    let group = this.consumerGroups.get(groupKey);

    if (!group) {
      const consumer = this.kafkaCore.getKafka(connectionName).consumer({
        groupId,
        sessionTimeout: options.sessionTimeout,
        heartbeatInterval: options.heartbeatInterval,
        rebalanceTimeout: options.rebalanceTimeout,
        maxBytesPerPartition: 1048576,
        retry: options.retry
          ? {
            retries: options.retry.retries,
            maxRetryTime: options.retry.maxRetryTime,
            initialRetryTime: options.retry.initialRetryTime,
            factor: options.retry.factor,
            multiplier: options.retry.multiplier,
          }
          : undefined,
      });

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

    // Add topic handler to the group
    const handler = target[methodName].bind(target);
    group.topics.set(topic, { metadata, handler });

    this.logger.log(
      `Registered topic "${topic}" in group "${groupId}" (connection: ${connectionName})`,
    );

    // Register DLQ retry consumer if enabled
    if (options.dlq?.retry?.enabled) {
      try {
        this.dlqRetryService.registerDlqRetryConsumer(metadata);
      } catch (err) {
        this.logger.error(
          `Failed to register DLQ retry consumer for ${topic}`,
          err,
        );
      }
    }
  }

  private buildRestartOnFailure(
    retry?: ConsumerRetryOptions,
  ): ((error: Error) => Promise<boolean>) | undefined {
    if (!retry?.restartOnFailure) {
      return undefined;
    }

    if (typeof retry.restartOnFailure === 'function') {
      return retry.restartOnFailure;
    }

    return () => Promise.resolve(retry.restartOnFailure as boolean);
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
      for (const topic of topicList) {
        const topicHandler = topics.get(topic)!;
        await consumer.subscribe({
          topic,
          fromBeginning: topicHandler.metadata.options.fromBeginning,
        });
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
    const { consumer, topics, options } = group;
    const restartOnFailure = this.buildRestartOnFailure(options.retry);

    await consumer.run({
      autoCommit: options.autoCommit !== false,
      autoCommitInterval: options.autoCommitInterval,
      autoCommitThreshold: options.autoCommitThreshold,
      partitionsConsumedConcurrently: options.partitionsConsumedConcurrently,
      ...(restartOnFailure && { restartOnFailure }),
      eachMessage: async (payload: EachMessagePayload) => {
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

        try {
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

          this.dlqService.clearRetryState(message, topic, partition);
        } catch (error) {
          await this.handleError(message, error as Error, metadata, partition);
        }
      },
    });
  }

  private async startBatchGroupConsumer(group: ConsumerGroup): Promise<void> {
    const { consumer, topics, options } = group;
    const restartOnFailure = this.buildRestartOnFailure(options.retry);

    await consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: options.partitionsConsumedConcurrently,
      ...(restartOnFailure && { restartOnFailure }),
      eachBatch: async (payload: EachBatchPayload) => {
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
            );
          });

          for (const message of messages) {
            if (!payload.isRunning() || payload.isStale()) break;

            await accumulator.add(message);
            payload.resolveOffset(message.offset);
            await payload.heartbeat();
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
                await payload.heartbeat();
                continue;
              }
            }

            try {
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

              this.dlqService.clearRetryState(message, topic, partition);
            } catch (error) {
              await this.handleError(
                message,
                error as Error,
                metadata,
                partition,
              );
            }

            payload.resolveOffset(message.offset);
            await payload.heartbeat();
          }
        }
      },
    });
  }

  private async processBatchMessages(
    messages: any[],
    topic: string,
    partition: number,
    options: ConsumerOptions,
    handler: (...args: any[]) => Promise<void>,
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

    if (options.groupByKey) {
      const grouped =
        this.batchProcessor.groupMessagesByKey(deserializedMessages);
      await handler(grouped);
    } else {
      await handler(deserializedMessages);
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
      throw error;
    }
  }

  async gracefulShutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.logger.log('Starting graceful shutdown of consumers...');

    const shutdownPromises = Array.from(this.consumerGroups.values()).map(
      async (group) => {
        try {
          if (group.isRunning) {
            await group.consumer.stop();
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

    this.logger.log('All consumer groups shut down gracefully');
  }

  async onApplicationShutdown(): Promise<void> {
    await this.gracefulShutdown();
  }
}
