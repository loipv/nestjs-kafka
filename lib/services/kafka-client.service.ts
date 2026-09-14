import {
  Injectable,
  OnApplicationShutdown,
  Logger,
  Optional,
} from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  ProducerMessage,
  SendOptions,
  DEFAULT_KAFKA_CONNECTION,
} from '../interfaces';

type ProducerRecord = KafkaJS.ProducerRecord;
type Message = KafkaJS.Message;
import { KafkaCoreService } from './kafka-core.service';
import { TracingService } from './tracing.service';

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

  constructor(
    private readonly kafkaCore: KafkaCoreService,
    @Optional() private readonly tracingService?: TracingService,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    try {
      await this.flushAllBatches();
    } catch (error) {
      this.logger.error('Failed to flush queued batches during shutdown', error);
    }
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

    // Start tracing span and inject trace context into headers
    const { span, headers } = this.tracingService?.startProduceSpan({
      topic,
      key: message.key ? String(message.key) : null,
      headers: message.headers,
    }) ?? { span: null, headers: message.headers || {} };

    const kafkaMessage = this.serializeMessage({
      ...message,
      headers, // Use headers with trace context
    });

    const record: ProducerRecord = {
      topic,
      messages: [kafkaMessage],
    };

    try {
      const producer = this.kafkaCore.getProducer(connectionName);
      await producer.send(record);
      this.logger.debug(`[${connectionName}] Message sent to topic: ${topic}`);
      this.tracingService?.endProduceSpan(span);
    } catch (error) {
      this.logger.error(
        `[${connectionName}] Failed to send message to topic: ${topic}`,
        error,
      );
      this.tracingService?.endProduceSpan(span, error as Error);
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

    // Create spans and inject trace context for each message
    const spans: any[] = [];
    const kafkaMessages = messages.map((msg) => {
      const { span, headers } = this.tracingService?.startProduceSpan({
        topic,
        key: msg.key ? String(msg.key) : null,
        headers: msg.headers,
      }) ?? { span: null, headers: msg.headers || {} };

      spans.push(span);
      return this.serializeMessage({ ...msg, headers });
    });

    const record: ProducerRecord = {
      topic,
      messages: kafkaMessages,
    };

    try {
      const producer = this.kafkaCore.getProducer(connectionName);
      await producer.send(record);
      this.logger.debug(
        `[${connectionName}] Batch of ${messages.length} messages sent to topic: ${topic}`,
      );
      spans.forEach((span) => this.tracingService?.endProduceSpan(span));
    } catch (error) {
      this.logger.error(
        `[${connectionName}] Failed to send batch to topic: ${topic}`,
        error,
      );
      spans.forEach((span) =>
        this.tracingService?.endProduceSpan(span, error as Error),
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

    // Create spans and inject trace context for each message
    const allSpans: any[] = [];
    const batch = {
      topicMessages: topicMessages.map(({ topic, messages }) => ({
        topic,
        messages: messages.map((msg) => {
          const { span, headers } = this.tracingService?.startProduceSpan({
            topic,
            key: msg.key ? String(msg.key) : null,
            headers: msg.headers,
          }) ?? { span: null, headers: msg.headers || {} };

          allSpans.push(span);
          return this.serializeMessage({ ...msg, headers });
        }),
      })),
    };

    try {
      const producer = this.kafkaCore.getProducer(connectionName);
      await producer.sendBatch(batch);
      this.logger.debug(
        `[${connectionName}] Multi-topic batch sent to ${topicMessages.length} topics`,
      );
      allSpans.forEach((span) => this.tracingService?.endProduceSpan(span));
    } catch (error) {
      this.logger.error(
        `[${connectionName}] Failed to send multi-topic batch`,
        error,
      );
      allSpans.forEach((span) =>
        this.tracingService?.endProduceSpan(span, error as Error),
      );
      throw error;
    }
  }

  /**
   * Queue a message for batched sending
   * Note: Trace context is injected at queue time to capture the caller's context
   */
  async sendQueued(
    topic: string,
    message: ProducerMessage,
    connection?: string,
  ): Promise<void> {
    const connectionName = connection || DEFAULT_KAFKA_CONNECTION;
    await this.kafkaCore.connectProducer(connectionName);

    // Inject trace context into headers at queue time
    const { headers } = this.tracingService?.startProduceSpan({
      topic,
      key: message.key ? String(message.key) : null,
      headers: message.headers,
    }) ?? { span: null, headers: message.headers || {} };

    // Note: We don't track spans for queued messages since they're flushed asynchronously
    // The trace context is captured in headers for downstream consumers to use
    const kafkaMessage = this.serializeMessage({ ...message, headers });

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
   * Check if a specific connection is healthy (producer actually connected).
   * Use checkBrokers() on KafkaHealthIndicator for an active connectivity probe.
   */
  isHealthy(connection?: string): boolean {
    return this.kafkaCore.isProducerConnected(connection);
  }

  private serializeMessage(message: ProducerMessage): Message {
    let value: Buffer | string | null;

    if (message.value === null || message.value === undefined) {
      value = null;
    } else if (Buffer.isBuffer(message.value)) {
      value = message.value;
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
      this.flushConnectionBatches(connectionName)
        .catch((err: unknown) => {
          this.logger.error(
            `Failed to flush batch for connection "${connectionName}"`,
            err,
          );
        })
        .finally(() => {
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
      connections.map(async (conn) => {
        try {
          await this.flushConnectionBatches(conn);
        } catch (error) {
          this.logger.error(
            `Failed to flush batches for connection "${conn}"`,
            error,
          );
        }
      }),
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
