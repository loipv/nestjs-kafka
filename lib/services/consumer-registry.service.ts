/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common';
import { Consumer, EachMessagePayload } from 'kafkajs';
import { KafkaCoreService } from './kafka-core.service';
import { BatchProcessorService } from './batch-processor.service';
import { IdempotencyService } from './idempotency.service';
import { PressureManagerService } from './pressure-manager.service';
import { DlqService } from './dlq.service';
import { ConsumerMetadata, deserializeMessage } from '../interfaces';

interface RegisteredConsumer {
  metadata: ConsumerMetadata;
  consumer: Consumer;
  isRunning: boolean;
}

@Injectable()
export class ConsumerRegistryService implements OnApplicationShutdown {
  private readonly logger = new Logger(ConsumerRegistryService.name);
  private registeredConsumers = new Map<string, RegisteredConsumer>();
  private isShuttingDown = false;

  constructor(
    private readonly kafkaCore: KafkaCoreService,
    private readonly batchProcessor: BatchProcessorService,
    private readonly idempotencyService: IdempotencyService,
    private readonly pressureManager: PressureManagerService,
    private readonly dlqService: DlqService,
  ) {}

  registerConsumers(consumers: ConsumerMetadata[]): void {
    for (const metadata of consumers) {
      this.registerConsumer(metadata);
    }
  }

  private registerConsumer(metadata: ConsumerMetadata): void {
    const { topic, options } = metadata;
    const consumerId = `${topic}-${options.groupId || 'default'}`;

    const consumer = this.kafkaCore.getKafka().consumer({
      groupId: options.groupId || `${topic}-group`,
      sessionTimeout: options.sessionTimeout,
      heartbeatInterval: options.heartbeatInterval,
      rebalanceTimeout: options.rebalanceTimeout,
      maxBytesPerPartition: 1048576,
    });

    this.pressureManager.register(consumerId, consumer, {
      backPressureThreshold: options.backPressureThreshold || 80,
      resumeThreshold: 60,
      maxQueueSize: options.maxQueueSize || 1000,
      checkIntervalMs: 1000,
    });

    this.registeredConsumers.set(consumerId, {
      metadata,
      consumer,
      isRunning: false,
    });

    this.logger.log(`Registered consumer: ${consumerId}`);
  }

  async startAll(): Promise<void> {
    const startPromises = Array.from(this.registeredConsumers.values()).map(
      (registered) => this.startConsumer(registered),
    );

    await Promise.all(startPromises);
  }

  private async startConsumer(registered: RegisteredConsumer): Promise<void> {
    const { metadata, consumer } = registered;
    const { topic, options } = metadata;

    try {
      await consumer.connect();

      await consumer.subscribe({
        topic,
        fromBeginning: options.fromBeginning,
      });

      if (options.batch) {
        await this.startBatchConsumer(registered);
      } else {
        await this.startMessageConsumer(registered);
      }

      registered.isRunning = true;
      this.logger.log(`Started consumer for topic: ${topic}`);
    } catch (error) {
      this.logger.error(`Failed to start consumer for topic: ${topic}`, error);
      throw error;
    }
  }

  private async startMessageConsumer(
    registered: RegisteredConsumer,
  ): Promise<void> {
    const { metadata, consumer } = registered;

    const { topic, options, target, methodName } = metadata;

    const handler = target[methodName].bind(target);

    await consumer.run({
      autoCommit: options.autoCommit !== false,
      autoCommitInterval: options.autoCommitInterval,
      autoCommitThreshold: options.autoCommitThreshold,
      eachMessage: async (payload: EachMessagePayload) => {
        if (this.isShuttingDown) return;

        const { message, partition } = payload;

        if (options.idempotencyKey) {
          if (
            this.idempotencyService.isProcessed(message, options.idempotencyKey)
          ) {
            this.logger.debug('Skipping duplicate message');
            return;
          }
        }

        try {
          // Auto-deserialize message if enabled (default: true)
          const processedMessage =
            options.deserialize !== false
              ? deserializeMessage(message, topic, partition)
              : message;

          await handler(processedMessage);

          if (options.idempotencyKey) {
            this.idempotencyService.markProcessed(
              message,
              options.idempotencyKey,
            );
          }

          this.dlqService.clearRetryState(message, topic, partition);
        } catch (error) {
          await this.handleError(message, error as Error, metadata, partition);
        }
      },
    });
  }

  private async startBatchConsumer(
    registered: RegisteredConsumer,
  ): Promise<void> {
    const { metadata, consumer } = registered;

    const { options, target, methodName } = metadata;

    const handler = target[methodName].bind(target);

    const eachBatchHandler = this.batchProcessor.createEachBatchHandler(
      options,
      async (messages, topic, partition) => {
        if (this.isShuttingDown) return;

        let processableMessages = messages;
        if (options.idempotencyKey) {
          processableMessages = this.idempotencyService.filterDuplicates(
            messages,
            options.idempotencyKey,
          );
        }

        // Auto-deserialize messages if enabled (default: true)
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
      },
    );

    await consumer.run({
      autoCommit: false,
      eachBatch: eachBatchHandler,
    });
  }

  private async handleError(
    message: any,
    error: Error,
    metadata: ConsumerMetadata,
    partition?: number,
  ): Promise<void> {
    const { topic, options } = metadata;

    this.logger.error(`Error processing message from ${topic}`, error);

    if (options.dlq) {
      const shouldRetry = await this.dlqService.handleFailure(
        message,
        error,
        options.dlq,
        topic,
        partition,
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

    const shutdownPromises = Array.from(this.registeredConsumers.values()).map(
      async (registered) => {
        try {
          if (registered.isRunning) {
            await registered.consumer.stop();
            await registered.consumer.disconnect();
          }
        } catch (error) {
          this.logger.error('Error during consumer shutdown', error);
        }
      },
    );

    await Promise.all(shutdownPromises);
    this.idempotencyService.stopCleanup();
    this.logger.log('All consumers shut down gracefully');
  }

  async onApplicationShutdown(): Promise<void> {
    await this.gracefulShutdown();
  }
}
