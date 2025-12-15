import { Injectable, OnApplicationShutdown, Logger } from '@nestjs/common';
import { ProducerRecord, Message } from 'kafkajs';
import {
  ProducerMessage,
  SendOptions,
  DEFAULT_KAFKA_CONNECTION,
} from '../interfaces';
import { KafkaCoreService } from './kafka-core.service';

export interface SendOptionsWithConnection extends SendOptions {
  /** Connection name to use. Default: 'default' */
  connection?: string;
}

@Injectable()
export class KafkaClient implements OnApplicationShutdown {
  private readonly logger = new Logger(KafkaClient.name);

  private batchBuffers = new Map<string, Map<string, Message[]>>();
  private batchTimers = new Map<string, NodeJS.Timeout>();
  private readonly defaultBatchSize = 100;
  private readonly defaultBatchTimeout = 100;

  constructor(private readonly kafkaCore: KafkaCoreService) {}

  async onApplicationShutdown(): Promise<void> {
    await this.flushAllBatches();
    await this.kafkaCore.disconnectAll();
  }

  /**
   * Send a single message to a topic
   */
  async send(
    topic: string,
    message: ProducerMessage,
    options?: SendOptionsWithConnection,
  ): Promise<void> {
    const connectionName = options?.connection || DEFAULT_KAFKA_CONNECTION;
    await this.kafkaCore.connectProducer(connectionName);

    const kafkaMessage = this.serializeMessage(message);

    const record: ProducerRecord = {
      topic,
      messages: [kafkaMessage],
      acks: options?.acks,
      timeout: options?.timeout,
      compression: options?.compression,
    };

    try {
      const producer = this.kafkaCore.getProducer(connectionName);
      await producer.send(record);
      this.logger.debug(`[${connectionName}] Message sent to topic: ${topic}`);
    } catch (error) {
      this.logger.error(
        `[${connectionName}] Failed to send message to topic: ${topic}`,
        error,
      );
      throw error;
    }
  }

  /**
   * Send a batch of messages to a single topic
   */
  async sendBatch(
    topic: string,
    messages: ProducerMessage[],
    options?: SendOptionsWithConnection,
  ): Promise<void> {
    const connectionName = options?.connection || DEFAULT_KAFKA_CONNECTION;
    await this.kafkaCore.connectProducer(connectionName);

    const kafkaMessages = messages.map((msg) => this.serializeMessage(msg));

    const record: ProducerRecord = {
      topic,
      messages: kafkaMessages,
      acks: options?.acks,
      timeout: options?.timeout,
      compression: options?.compression,
    };

    try {
      const producer = this.kafkaCore.getProducer(connectionName);
      await producer.send(record);
      this.logger.debug(
        `[${connectionName}] Batch of ${messages.length} messages sent to topic: ${topic}`,
      );
    } catch (error) {
      this.logger.error(
        `[${connectionName}] Failed to send batch to topic: ${topic}`,
        error,
      );
      throw error;
    }
  }

  /**
   * Send messages to multiple topics in a single batch
   */
  async sendMultiTopicBatch(
    topicMessages: Array<{ topic: string; messages: ProducerMessage[] }>,
    options?: SendOptionsWithConnection,
  ): Promise<void> {
    const connectionName = options?.connection || DEFAULT_KAFKA_CONNECTION;
    await this.kafkaCore.connectProducer(connectionName);

    const batch = {
      topicMessages: topicMessages.map(({ topic, messages }) => ({
        topic,
        messages: messages.map((msg) => this.serializeMessage(msg)),
      })),
      acks: options?.acks,
      timeout: options?.timeout,
      compression: options?.compression,
    };

    try {
      const producer = this.kafkaCore.getProducer(connectionName);
      await producer.sendBatch(batch);
      this.logger.debug(
        `[${connectionName}] Multi-topic batch sent to ${topicMessages.length} topics`,
      );
    } catch (error) {
      this.logger.error(
        `[${connectionName}] Failed to send multi-topic batch`,
        error,
      );
      throw error;
    }
  }

  /**
   * Queue a message for batched sending
   */
  async sendQueued(
    topic: string,
    message: ProducerMessage,
    connection?: string,
  ): Promise<void> {
    const connectionName = connection || DEFAULT_KAFKA_CONNECTION;
    await this.kafkaCore.connectProducer(connectionName);

    const kafkaMessage = this.serializeMessage(message);

    // Get or create buffer for this connection
    if (!this.batchBuffers.has(connectionName)) {
      this.batchBuffers.set(connectionName, new Map());
    }

    const connectionBuffer = this.batchBuffers.get(connectionName)!;
    if (!connectionBuffer.has(topic)) {
      connectionBuffer.set(topic, []);
    }

    connectionBuffer.get(topic)!.push(kafkaMessage);

    const buffer = connectionBuffer.get(topic)!;
    if (buffer.length >= this.defaultBatchSize) {
      await this.flushBatch(connectionName, topic);
    } else {
      this.scheduleBatchFlush(connectionName);
    }
  }

  /**
   * Get a client for a specific connection (for fluent API)
   */
  forConnection(name: string): ConnectionBoundClient {
    return new ConnectionBoundClient(this, name);
  }

  /**
   * Check if a specific connection is healthy
   */
  isHealthy(connection?: string): boolean {
    return this.kafkaCore.hasConnection(connection);
  }

  private serializeMessage(message: ProducerMessage): Message {
    let value: Buffer | string | null;

    if (message.value === null || message.value === undefined) {
      value = null;
    } else if (typeof message.value === 'object') {
      value = JSON.stringify(message.value);
    } else {
      value = String(message.value);
    }

    return {
      key: message.key ? String(message.key) : null,
      value,
      headers: message.headers,
      partition: message.partition,
      timestamp: message.timestamp,
    };
  }

  private scheduleBatchFlush(connectionName: string): void {
    const timerKey = connectionName;
    if (this.batchTimers.has(timerKey)) return;

    const timer = setTimeout(() => {
      void this.flushConnectionBatches(connectionName).then(() => {
        this.batchTimers.delete(timerKey);
      });
    }, this.defaultBatchTimeout);

    this.batchTimers.set(timerKey, timer);
  }

  private async flushBatch(
    connectionName: string,
    topic: string,
  ): Promise<void> {
    const connectionBuffer = this.batchBuffers.get(connectionName);
    if (!connectionBuffer) return;

    const messages = connectionBuffer.get(topic);
    if (!messages || messages.length === 0) return;

    connectionBuffer.set(topic, []);

    const producer = this.kafkaCore.getProducer(connectionName);
    await producer.send({
      topic,
      messages,
    });
  }

  private async flushConnectionBatches(connectionName: string): Promise<void> {
    const connectionBuffer = this.batchBuffers.get(connectionName);
    if (!connectionBuffer) return;

    const topics = Array.from(connectionBuffer.keys());
    await Promise.all(
      topics.map((topic) => this.flushBatch(connectionName, topic)),
    );
  }

  private async flushAllBatches(): Promise<void> {
    const connections = Array.from(this.batchBuffers.keys());
    await Promise.all(
      connections.map((conn) => this.flushConnectionBatches(conn)),
    );

    // Clear all timers
    for (const timer of this.batchTimers.values()) {
      clearTimeout(timer);
    }
    this.batchTimers.clear();
  }
}

/**
 * A client bound to a specific connection for fluent API usage
 */
export class ConnectionBoundClient {
  constructor(
    private readonly client: KafkaClient,
    private readonly connection: string,
  ) {}

  async send(
    topic: string,
    message: ProducerMessage,
    options?: SendOptions,
  ): Promise<void> {
    return this.client.send(topic, message, {
      ...options,
      connection: this.connection,
    });
  }

  async sendBatch(
    topic: string,
    messages: ProducerMessage[],
    options?: SendOptions,
  ): Promise<void> {
    return this.client.sendBatch(topic, messages, {
      ...options,
      connection: this.connection,
    });
  }

  async sendMultiTopicBatch(
    topicMessages: Array<{ topic: string; messages: ProducerMessage[] }>,
    options?: SendOptions,
  ): Promise<void> {
    return this.client.sendMultiTopicBatch(topicMessages, {
      ...options,
      connection: this.connection,
    });
  }

  async sendQueued(topic: string, message: ProducerMessage): Promise<void> {
    return this.client.sendQueued(topic, message, this.connection);
  }
}
