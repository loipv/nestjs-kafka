import { Injectable, Logger, forwardRef, Inject } from '@nestjs/common';
import { KafkaMessage, IHeaders } from 'kafkajs';
import { KafkaClient } from './kafka-client.service';
import { DlqOptions } from '../interfaces';

interface RetryState {
  retryCount: number;
  lastError?: Error;
}

@Injectable()
export class DlqService {
  private readonly logger = new Logger(DlqService.name);
  private retryStates = new Map<string, RetryState>();

  constructor(
    @Inject(forwardRef(() => KafkaClient))
    private readonly kafkaClient: KafkaClient,
  ) {}

  async handleFailure(
    message: KafkaMessage,
    error: Error,
    options: DlqOptions,
    originalTopic: string,
    partition?: number,
  ): Promise<boolean> {
    const messageKey = this.getMessageKey(message, originalTopic, partition);
    let state = this.retryStates.get(messageKey);

    if (!state) {
      state = { retryCount: 0 };
      this.retryStates.set(messageKey, state);
    }

    state.retryCount++;
    state.lastError = error;

    const maxRetries = options.maxRetries ?? 3;

    if (state.retryCount <= maxRetries) {
      const baseDelay = options.retryDelay ?? 1000;
      const multiplier = options.retryBackoffMultiplier ?? 2;
      const delay = baseDelay * Math.pow(multiplier, state.retryCount - 1);

      this.logger.warn(
        `Retry ${state.retryCount}/${maxRetries} for message, waiting ${delay}ms`,
      );

      await this.sleep(delay);
      return true;
    }

    await this.sendToDlq(
      message,
      error,
      options,
      originalTopic,
      state.retryCount,
    );
    this.retryStates.delete(messageKey);

    return false;
  }

  private async sendToDlq(
    message: KafkaMessage,
    error: Error,
    options: DlqOptions,
    originalTopic: string,
    retryCount: number,
  ): Promise<void> {
    const headers: IHeaders = {};

    if (options.includeOriginalHeaders !== false && message.headers) {
      Object.assign(headers, message.headers);
    }

    headers['x-dlq-original-topic'] = originalTopic;
    headers['x-dlq-retry-count'] = String(retryCount);
    headers['x-dlq-timestamp'] = new Date().toISOString();

    if (options.includeErrorInfo !== false) {
      headers['x-dlq-error-message'] = error.message;
      headers['x-dlq-error-stack'] = error.stack || '';
    }

    try {
      await this.kafkaClient.send(options.topic, {
        key: message.key,
        value: message.value,
        headers,
      });

      this.logger.warn(
        `Message sent to DLQ: ${options.topic} after ${retryCount} retries`,
      );
    } catch (dlqError) {
      this.logger.error('Failed to send message to DLQ', dlqError);
      throw dlqError;
    }
  }

  private getMessageKey(
    message: KafkaMessage,
    topic: string,
    partition?: number,
  ): string {
    return `${topic}:${partition ?? 0}:${message.offset}`;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  clearRetryState(
    message: KafkaMessage,
    topic: string,
    partition?: number,
  ): void {
    const key = this.getMessageKey(message, topic, partition);
    this.retryStates.delete(key);
  }
}
