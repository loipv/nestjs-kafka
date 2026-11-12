import { Injectable, Logger } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaClient } from './kafka-client.service';

type KafkaMessage = KafkaJS.KafkaMessage;
type IHeaders = KafkaJS.IHeaders;
import { DlqOptions, RetryVerdict, MAX_RETRY_DELAY_MS } from '../interfaces';

// Header constants for clarity
export const DLQ_HEADERS = {
  ORIGINAL_TOPIC: 'x-dlq-original-topic',
  HANDLER_RETRY_COUNT: 'x-dlq-handler-retry-count', // Renamed from x-dlq-retry-count
  TIMESTAMP: 'x-dlq-timestamp',
  ERROR_MESSAGE: 'x-dlq-error-message',
  ERROR_STACK: 'x-dlq-error-stack',
} as const;

@Injectable()
export class DlqService {
  private readonly logger = new Logger(DlqService.name);

  constructor(private readonly kafkaClient: KafkaClient) {}

  /**
   * Decide what to do after a handler failure. Stateless: the caller supplies
   * the current attempt number and owns the retry loop (and its delay/sleep).
   *
   * - attempt <= maxRetries → { action: 'retry', delayMs } (capped backoff)
   * - otherwise              → sends to DLQ → { action: 'complete' }
   * - DLQ send failure       → REJECTS: the client redelivers the message and
   *                            the retry ladder starts again. Nothing is dropped.
   */
  async handleFailure(
    message: KafkaMessage,
    error: Error,
    options: DlqOptions,
    originalTopic: string,
    partition?: number,
    connection?: string,
    attempt = 1,
  ): Promise<RetryVerdict> {
    const maxRetries = options.maxRetries ?? 3;

    if (attempt <= maxRetries) {
      const baseDelay = options.retryDelay ?? 1000;
      const multiplier = options.retryBackoffMultiplier ?? 2;
      const delayMs = Math.min(
        baseDelay * multiplier ** (attempt - 1),
        MAX_RETRY_DELAY_MS,
      );
      this.logger.warn(
        `Retry ${attempt}/${maxRetries} for message from ${originalTopic}, waiting ${delayMs}ms`,
      );
      return { action: 'retry', delayMs };
    }

    await this.sendToDlq(
      message,
      error,
      options,
      originalTopic,
      attempt,
      connection,
    );
    return { action: 'complete' };
  }

  async sendToDlq(
    message: KafkaMessage,
    error: Error,
    options: DlqOptions,
    originalTopic: string,
    retryCount: number,
    connection?: string,
  ): Promise<void> {
    const headers: IHeaders = {};

    if (options.includeOriginalHeaders !== false && message.headers) {
      Object.assign(headers, message.headers);
    }

    // Use new header names
    headers[DLQ_HEADERS.ORIGINAL_TOPIC] = originalTopic;
    headers[DLQ_HEADERS.HANDLER_RETRY_COUNT] = String(retryCount);
    headers[DLQ_HEADERS.TIMESTAMP] = new Date().toISOString();

    if (options.includeErrorInfo !== false) {
      const MAX_HEADER_LENGTH = 1000;
      headers[DLQ_HEADERS.ERROR_MESSAGE] = (error.message || '').substring(
        0,
        MAX_HEADER_LENGTH,
      );
      headers[DLQ_HEADERS.ERROR_STACK] = (error.stack || '').substring(
        0,
        MAX_HEADER_LENGTH,
      );
    }

    try {
      const messageValue = message.value
        ? message.value.toString('utf-8')
        : null;
      const messageKey = message.key
        ? Buffer.isBuffer(message.key)
          ? message.key.toString('utf-8')
          : message.key
        : null;

      const target = options.connection || connection;
      await this.kafkaClient.send(
        options.topic,
        {
          key: messageKey,
          value: messageValue,
          headers,
        },
        target ? { connection: target } : undefined,
      );

      this.logger.warn(
        `Message sent to DLQ: ${options.topic} after ${retryCount} retries`,
      );
    } catch (dlqError) {
      this.logger.error('Failed to send message to DLQ', dlqError);
      throw dlqError;
    }
  }
}
