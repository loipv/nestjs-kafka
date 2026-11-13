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

const DEFAULT_SEND_TIMEOUT_MS = 30000;
const MAX_HEADER_LENGTH = 1000;

@Injectable()
export class DlqService {
  private readonly logger = new Logger(DlqService.name);

  constructor(private readonly kafkaClient: KafkaClient) {}

  /**
   * Decide what to do after a handler failure. Stateless: the caller supplies
   * the current attempt number and owns the retry (pause, resume).
   *
   * - attempt <= maxRetries → { action: 'retry', delayMs } (capped backoff)
   * - otherwise              → dead-letters `messages` → { action: 'complete' }
   * - DLQ send failure or timeout → { action: 'retry', delayMs }. Never rejects:
   *   the message is retried and nothing is dropped.
   */
  async handleFailure(
    messages: KafkaMessage[],
    error: Error,
    options: DlqOptions,
    originalTopic: string,
    partition: number | undefined,
    connection: string | undefined,
    attempt: number,
    maxDelayMs: number = MAX_RETRY_DELAY_MS,
    sent: Set<string> = new Set(),
  ): Promise<RetryVerdict> {
    const maxRetries = options.maxRetries ?? 3;
    const delayMs = this.retryDelay(attempt, options, maxDelayMs);

    if (attempt <= maxRetries) {
      this.logger.warn(
        `Retry ${attempt}/${maxRetries} for message from ${originalTopic}` +
          `${partition !== undefined ? ` [${partition}]` : ''}, waiting ${delayMs}ms`,
      );
      return { action: 'retry', delayMs };
    }

    try {
      await this.sendToDlq(
        messages,
        error,
        options,
        originalTopic,
        attempt,
        connection,
        sent,
      );
      return { action: 'complete' };
    } catch (dlqError) {
      this.logger.error('Failed to send message to DLQ', dlqError);
      return { action: 'retry', delayMs };
    }
  }

  /**
   * Dead-letter `messages`, one send per message in parallel (librdkafka
   * batches them). Values and keys pass through as received (Buffers stay
   * byte-exact). Offsets in `sent` are skipped and each success is added to
   * it, so a retry never re-sends what already arrived. Rejects when any send
   * fails or the sends exceed `options.sendTimeout`.
   */
  async sendToDlq(
    messages: KafkaMessage[],
    error: Error,
    options: DlqOptions,
    originalTopic: string,
    retryCount: number,
    connection?: string,
    sent: Set<string> = new Set(),
  ): Promise<void> {
    const pending = messages.filter((message) => !sent.has(message.offset));
    if (pending.length === 0) return;

    const timestamp = new Date().toISOString();
    const records = pending.map((message) => ({
      key: message.key ?? null,
      value: message.value ?? null,
      headers: this.buildHeaders(
        message,
        error,
        options,
        originalTopic,
        retryCount,
        timestamp,
      ),
    }));

    const target = options.connection || connection;
    const timeoutMs = options.sendTimeout ?? DEFAULT_SEND_TIMEOUT_MS;
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`DLQ send timed out after ${timeoutMs}ms`)),
        timeoutMs,
      );
    });

    const sendAll = async () => {
      const results = await Promise.allSettled(
        records.map((record) =>
          this.kafkaClient.send(
            options.topic,
            record,
            target ? { connection: target } : undefined,
          ),
        ),
      );
      const failures: unknown[] = [];
      results.forEach((result, i) => {
        if (result.status === 'fulfilled') sent.add(pending[i].offset);
        else failures.push(result.reason);
      });
      if (failures.length > 0) {
        throw new Error(
          `DLQ send failed for ${failures.length} of ${pending.length} message(s): ${String(failures[0])}`,
        );
      }
    };

    try {
      await Promise.race([sendAll(), timeout]);
    } finally {
      clearTimeout(timer);
    }

    this.logger.warn(
      `${pending.length} message(s) sent to DLQ: ${options.topic} after ${retryCount} attempts`,
    );
  }

  private retryDelay(
    attempt: number,
    options: DlqOptions,
    maxDelayMs: number,
  ): number {
    const baseDelay = options.retryDelay ?? 1000;
    const multiplier = options.retryBackoffMultiplier ?? 2;
    return Math.min(baseDelay * multiplier ** (attempt - 1), maxDelayMs);
  }

  private buildHeaders(
    message: KafkaMessage,
    error: Error,
    options: DlqOptions,
    originalTopic: string,
    retryCount: number,
    timestamp: string,
  ): IHeaders {
    const headers: IHeaders = {};

    if (options.includeOriginalHeaders !== false && message.headers) {
      Object.assign(headers, message.headers);
    }

    headers[DLQ_HEADERS.ORIGINAL_TOPIC] = originalTopic;
    headers[DLQ_HEADERS.HANDLER_RETRY_COUNT] = String(retryCount);
    headers[DLQ_HEADERS.TIMESTAMP] = timestamp;

    if (options.includeErrorInfo !== false) {
      headers[DLQ_HEADERS.ERROR_MESSAGE] = (error.message || '').substring(
        0,
        MAX_HEADER_LENGTH,
      );
      headers[DLQ_HEADERS.ERROR_STACK] = (error.stack || '').substring(
        0,
        MAX_HEADER_LENGTH,
      );
    }

    return headers;
  }
}
