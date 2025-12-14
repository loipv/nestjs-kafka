import {
  Injectable,
  OnModuleInit,
  OnApplicationShutdown,
  Inject,
  Logger,
} from '@nestjs/common';
import { Producer, ProducerRecord, Message } from 'kafkajs';
import {
  KafkaModuleOptions,
  KAFKA_MODULE_OPTIONS,
  ProducerMessage,
  SendOptions,
} from '../interfaces';
import { KafkaCoreService } from './kafka-core.service';

@Injectable()
export class KafkaClient implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(KafkaClient.name);
  private producer: Producer;
  private isConnected = false;

  private batchBuffer: Map<string, Message[]> = new Map();
  private batchTimer: NodeJS.Timeout | null = null;
  private readonly defaultBatchSize = 100;
  private readonly defaultBatchTimeout = 100;

  constructor(
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
    private readonly kafkaCore: KafkaCoreService,
  ) {}

  async onModuleInit(): Promise<void> {
    this.producer = this.kafkaCore.getKafka().producer(this.options.producer);
    await this.connect();
  }

  async onApplicationShutdown(): Promise<void> {
    await this.disconnect();
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;

    try {
      await this.producer.connect();
      this.isConnected = true;
      this.logger.log('Kafka producer connected');
    } catch (error) {
      this.logger.error('Failed to connect Kafka producer', error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    await this.flushAllBatches();

    if (this.producer && this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
      this.logger.log('Kafka producer disconnected');
    }
  }

  async send(
    topic: string,
    message: ProducerMessage,
    options?: SendOptions,
  ): Promise<void> {
    const kafkaMessage = this.serializeMessage(message);

    const record: ProducerRecord = {
      topic,
      messages: [kafkaMessage],
      acks: options?.acks,
      timeout: options?.timeout,
      compression: options?.compression,
    };

    try {
      await this.producer.send(record);
      this.logger.debug(`Message sent to topic: ${topic}`);
    } catch (error) {
      this.logger.error(`Failed to send message to topic: ${topic}`, error);
      throw error;
    }
  }

  async sendBatch(
    topic: string,
    messages: ProducerMessage[],
    options?: SendOptions,
  ): Promise<void> {
    const kafkaMessages = messages.map((msg) => this.serializeMessage(msg));

    const record: ProducerRecord = {
      topic,
      messages: kafkaMessages,
      acks: options?.acks,
      timeout: options?.timeout,
      compression: options?.compression,
    };

    try {
      await this.producer.send(record);
      this.logger.debug(
        `Batch of ${messages.length} messages sent to topic: ${topic}`,
      );
    } catch (error) {
      this.logger.error(`Failed to send batch to topic: ${topic}`, error);
      throw error;
    }
  }

  async sendMultiTopicBatch(
    topicMessages: Array<{ topic: string; messages: ProducerMessage[] }>,
    options?: SendOptions,
  ): Promise<void> {
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
      await this.producer.sendBatch(batch);
      this.logger.debug(
        `Multi-topic batch sent to ${topicMessages.length} topics`,
      );
    } catch (error) {
      this.logger.error('Failed to send multi-topic batch', error);
      throw error;
    }
  }

  async sendQueued(topic: string, message: ProducerMessage): Promise<void> {
    const kafkaMessage = this.serializeMessage(message);

    if (!this.batchBuffer.has(topic)) {
      this.batchBuffer.set(topic, []);
    }

    this.batchBuffer.get(topic)!.push(kafkaMessage);

    const buffer = this.batchBuffer.get(topic)!;
    if (buffer.length >= this.defaultBatchSize) {
      await this.flushBatch(topic);
    } else {
      this.scheduleBatchFlush();
    }
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

  private scheduleBatchFlush(): void {
    if (this.batchTimer) return;

    this.batchTimer = setTimeout(() => {
      void this.flushAllBatches().then(() => {
        this.batchTimer = null;
      });
    }, this.defaultBatchTimeout);
  }

  private async flushBatch(topic: string): Promise<void> {
    const messages = this.batchBuffer.get(topic);
    if (!messages || messages.length === 0) return;

    this.batchBuffer.set(topic, []);

    await this.producer.send({
      topic,
      messages,
    });
  }

  private async flushAllBatches(): Promise<void> {
    const topics = Array.from(this.batchBuffer.keys());
    await Promise.all(topics.map((topic) => this.flushBatch(topic)));
  }

  isHealthy(): boolean {
    return this.isConnected;
  }
}
