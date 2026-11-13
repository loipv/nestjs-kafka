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

@Injectable()
export class KafkaClient implements OnApplicationShutdown {
  private readonly logger = new Logger(KafkaClient.name);

  constructor(
    private readonly kafkaCore: KafkaCoreService,
    @Optional() private readonly tracingService?: TracingService,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    await this.kafkaCore.disconnectAll();
  }

  /**
   * Send a single message to a topic
   */
  async send(
    topic: string,
    message: ProducerMessage,
    options?: SendOptions,
  ): Promise<void> {
    return this.sendOne(topic, message, options, true);
  }

  /**
   * `send()` without the error log: for callers that aggregate failures
   * themselves (the DLQ logs one line per failed chunk).
   * @internal
   */
  async sendSilently(
    topic: string,
    message: ProducerMessage,
    options?: SendOptions,
  ): Promise<void> {
    return this.sendOne(topic, message, options, false);
  }

  private async sendOne(
    topic: string,
    message: ProducerMessage,
    options: SendOptions | undefined,
    logFailure: boolean,
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
      if (logFailure) {
        this.logger.error(
          `[${connectionName}] Failed to send message to topic: ${topic}`,
          error,
        );
      }
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
    options?: SendOptions,
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
    options?: SendOptions,
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
   * Check if a specific connection is healthy (producer actually connected).
   * Use checkBrokers() on KafkaHealthIndicator for an active connectivity probe.
   */
  isHealthy(connection?: string): boolean {
    return this.kafkaCore.isProducerConnected(connection);
  }

  /** Strings and Buffers pass through (key 0 and '' included); null/undefined → null. */
  private serializeKey(key: unknown): string | Buffer | null {
    if (key === null || key === undefined) return null;
    if (typeof key === 'string' || Buffer.isBuffer(key)) return key;
    return String(key as number | bigint | boolean);
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
      key: this.serializeKey(message.key),
      value,
      headers: message.headers,
      partition: message.partition,
      timestamp: message.timestamp,
    };
  }
}

/**
 * A client bound to one named connection (what `@InjectKafkaClient(name)` injects)
 */
export class ConnectionBoundClient {
  constructor(
    private readonly client: KafkaClient,
    private readonly connection: string,
  ) {}

  async send(topic: string, message: ProducerMessage): Promise<void> {
    return this.client.send(topic, message, { connection: this.connection });
  }

  async sendBatch(topic: string, messages: ProducerMessage[]): Promise<void> {
    return this.client.sendBatch(topic, messages, {
      connection: this.connection,
    });
  }

  async sendMultiTopicBatch(
    topicMessages: Array<{ topic: string; messages: ProducerMessage[] }>,
  ): Promise<void> {
    return this.client.sendMultiTopicBatch(topicMessages, {
      connection: this.connection,
    });
  }
}
