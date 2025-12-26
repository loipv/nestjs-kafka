import {
    Injectable,
    Logger,
    forwardRef,
    Inject,
    OnApplicationShutdown,
} from '@nestjs/common';
import { Consumer, EachMessagePayload } from 'kafkajs';
import { KafkaClient } from './kafka-client.service';
import { KafkaCoreService } from './kafka-core.service';
import {
    DlqRetryOptions,
    ConsumerMetadata,
    DEFAULT_KAFKA_CONNECTION,
} from '../interfaces';

interface DlqTopicHandler {
    dlqTopic: string;
    retryOptions: DlqRetryOptions;
    originalTopic: string; // Fallback only, header takes precedence
}

interface DlqConsumerGroup {
    groupId: string;
    connection: string;
    consumer: Consumer;
    topics: Map<string, DlqTopicHandler>; // dlqTopic -> handler
    isRunning: boolean;
}

/**
 * Service that manages DLQ retry consumers.
 * Groups consumers by groupId similar to ConsumerRegistryService.
 * Automatically consumes messages from DLQ topics and re-publishes them
 * to the original topic after a delay.
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
    ) { }

    /**
     * Register an original consumer groupId to prevent DLQ groupId collision
     */
    registerOriginalGroupId(connection: string, groupId: string): void {
        this.originalConsumerGroupIds.add(`${connection}:${groupId}`);
    }

    /**
     * Register a DLQ retry consumer for a consumer metadata that has DLQ retry enabled
     */
    registerDlqRetryConsumer(metadata: ConsumerMetadata): void {
        const { options, topic } = metadata;
        const dlqOptions = options.dlq;

        if (!dlqOptions?.retry?.enabled) {
            return;
        }

        const dlqTopic = dlqOptions.topic;
        const retryOptions = dlqOptions.retry;
        const connection = options.connection || DEFAULT_KAFKA_CONNECTION;

        // Generate base groupId - use custom or default based on DLQ topic
        let groupId = retryOptions.groupId || `${dlqTopic}-retry-consumer`;

        // Check for collision with original consumer groupIds
        // If collision detected, append '-dlq' suffix to ensure uniqueness
        const groupKey = `${connection}:${groupId}`;
        if (this.originalConsumerGroupIds.has(groupKey)) {
            const newGroupId = `${groupId}-dlq`;
            this.logger.warn(
                `DLQ groupId "${groupId}" collides with original consumer groupId. Using "${newGroupId}" instead.`,
            );
            groupId = newGroupId;
        }

        // Final key by connection + groupId
        const finalGroupKey = `${connection}:${groupId}`;

        // Get or create consumer group
        let group = this.dlqConsumerGroups.get(finalGroupKey);

        if (!group) {
            const kafka = this.kafkaCore.getKafka(connection);
            const consumer = kafka.consumer({ groupId });

            group = {
                groupId,
                connection,
                consumer,
                topics: new Map(),
                isRunning: false,
            };

            this.dlqConsumerGroups.set(finalGroupKey, group);
            this.logger.log(
                `Created DLQ consumer group: ${groupId} (connection: ${connection})`,
            );
        }

        // Check if this DLQ topic is already registered in this group
        if (group.topics.has(dlqTopic)) {
            this.logger.warn(
                `DLQ topic ${dlqTopic} already registered in group ${groupId}, skipping`,
            );
            return;
        }

        // Add topic handler to the group
        group.topics.set(dlqTopic, {
            dlqTopic,
            retryOptions,
            originalTopic: topic, // Fallback
        });

        this.logger.log(
            `Registered DLQ topic "${dlqTopic}" in group "${groupId}" (connection: ${connection})`,
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
                const handler = topics.get(dlqTopic)!;
                await consumer.subscribe({
                    topic: dlqTopic,
                    fromBeginning: handler.retryOptions.fromBeginning ?? false,
                });
            }

            this.logger.log(
                `DLQ consumer group "${groupId}" subscribed to topics: ${topicList.join(', ')}`,
            );

            await consumer.run({
                eachMessage: async (payload: EachMessagePayload) => {
                    if (this.isShuttingDown) return;

                    const { topic: dlqTopic } = payload;

                    // Find the handler for this DLQ topic
                    const handler = topics.get(dlqTopic);
                    if (!handler) {
                        this.logger.warn(`No handler found for DLQ topic: ${dlqTopic}`);
                        return;
                    }

                    // Read original topic from message header (set by DlqService)
                    const headers = payload.message.headers || {};
                    const originalTopicHeader = headers['x-dlq-original-topic'];
                    const originalTopic = originalTopicHeader
                        ? originalTopicHeader.toString()
                        : handler.originalTopic; // Fallback to registered topic

                    await this.handleDlqMessage(
                        payload,
                        originalTopic,
                        handler.retryOptions,
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
     * Handle a message from DLQ - re-publish to original topic after delay
     */
    private async handleDlqMessage(
        payload: EachMessagePayload,
        originalTopic: string,
        retryOptions: DlqRetryOptions,
        connection: string,
        dlqTopic: string,
    ): Promise<void> {
        const { message } = payload;
        const headers = message.headers || {};

        // Get current DLQ retry count
        const dlqRetryCountHeader = headers['x-dlq-retry-from-dlq'];
        const currentDlqRetryCount = dlqRetryCountHeader
            ? parseInt(dlqRetryCountHeader.toString(), 10)
            : 0;

        const maxRetries = retryOptions.maxRetries ?? 3;
        const baseDelay = retryOptions.delay ?? 60000;
        const backoffMultiplier = retryOptions.backoffMultiplier ?? 2;

        // Check if we've exceeded max DLQ retries
        if (currentDlqRetryCount >= maxRetries) {
            await this.handleMaxRetriesExceeded(
                message,
                retryOptions,
                connection,
                dlqTopic,
                currentDlqRetryCount,
            );
            return;
        }

        // Calculate delay with exponential backoff
        const delay =
            baseDelay * Math.pow(backoffMultiplier, currentDlqRetryCount);

        this.logger.log(
            `DLQ retry ${currentDlqRetryCount + 1}/${maxRetries} for message from ${dlqTopic}, waiting ${delay}ms before re-publishing to ${originalTopic}`,
        );

        // Wait before re-publishing
        await this.sleep(delay);

        // Update headers for tracking
        const newHeaders = { ...headers };
        newHeaders['x-dlq-retry-from-dlq'] = String(currentDlqRetryCount + 1);
        newHeaders['x-dlq-retry-timestamp'] = new Date().toISOString();

        try {
            // Re-publish to original topic
            await this.kafkaClient.send(
                originalTopic,
                {
                    key: message.key,
                    value: message.value,
                    headers: newHeaders,
                },
                { connection },
            );

            this.logger.log(
                `Message re-published from ${dlqTopic} to ${originalTopic} (retry ${currentDlqRetryCount + 1}/${maxRetries})`,
            );
        } catch (error) {
            this.logger.error(
                `Failed to re-publish message from ${dlqTopic} to ${originalTopic}`,
                error,
            );
            throw error;
        }
    }

    /**
     * Handle message that has exceeded max DLQ retries
     */
    private async handleMaxRetriesExceeded(
        message: any,
        retryOptions: DlqRetryOptions,
        connection: string,
        dlqTopic: string,
        retryCount: number,
    ): Promise<void> {
        const finalDlqTopic = retryOptions.finalDlqTopic;

        if (finalDlqTopic) {
            // Send to final DLQ topic
            const headers = { ...(message.headers || {}) };
            headers['x-final-dlq-reason'] = 'max-dlq-retries-exceeded';
            headers['x-final-dlq-retry-count'] = String(retryCount);
            headers['x-final-dlq-timestamp'] = new Date().toISOString();
            headers['x-final-dlq-source'] = dlqTopic;

            try {
                await this.kafkaClient.send(
                    finalDlqTopic,
                    {
                        key: message.key,
                        value: message.value,
                        headers,
                    },
                    { connection },
                );

                this.logger.warn(
                    `Message sent to final DLQ ${finalDlqTopic} after ${retryCount} DLQ retries from ${dlqTopic}`,
                );
            } catch (error) {
                this.logger.error(
                    `Failed to send message to final DLQ ${finalDlqTopic}`,
                    error,
                );
                throw error;
            }
        } else {
            // No final DLQ configured - drop the message
            this.logger.warn(
                `Message dropped after ${retryCount} DLQ retries from ${dlqTopic} (no finalDlqTopic configured)`,
            );
        }
    }

    /**
     * Graceful shutdown - stop all DLQ consumer groups
     */
    async gracefulShutdown(): Promise<void> {
        this.isShuttingDown = true;
        this.logger.log('Starting graceful shutdown of DLQ consumers...');

        const shutdownPromises = Array.from(this.dlqConsumerGroups.values()).map(
            async (group) => {
                try {
                    if (group.isRunning) {
                        await group.consumer.stop();
                        await group.consumer.disconnect();
                        group.isRunning = false;
                        this.logger.log(
                            `DLQ consumer group "${group.groupId}" stopped`,
                        );
                    }
                } catch (error) {
                    this.logger.error(
                        `Error stopping DLQ consumer group: ${group.groupId}`,
                        error,
                    );
                }
            },
        );

        await Promise.all(shutdownPromises);
        this.logger.log('All DLQ consumer groups shut down gracefully');
    }

    async onApplicationShutdown(): Promise<void> {
        await this.gracefulShutdown();
    }

    private sleep(ms: number): Promise<void> {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}
