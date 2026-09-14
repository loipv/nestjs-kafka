import { Injectable, Logger } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaClient } from './kafka-client.service';

type KafkaMessage = KafkaJS.KafkaMessage;
type IHeaders = KafkaJS.IHeaders;
import {
  DlqOptions,
  RetryVerdict,
  MAX_RETRY_DELAY_MS,
} from '../interfaces';
import { DlqMetricsService } from './dlq-metrics.service';
import { CircuitBreakerService, CircuitState } from './circuit-breaker.service';

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

  constructor(
    private readonly kafkaClient: KafkaClient,
    private readonly metrics: DlqMetricsService,
    private readonly circuitBreaker: CircuitBreakerService,
  ) {}

  /**
   * Decide what to do after a handler failure. Stateless: the caller supplies
   * the current attempt number and owns the retry loop (and its delay/sleep).
   *
   * - attempt <= maxRetries → { action: 'retry', delayMs } (capped backoff)
   * - circuit breaker open   → { action: 'complete' } (message dropped, logged)
   * - otherwise              → sends to DLQ → { action: 'complete' }
   * - DLQ send failure       → REJECTS (infra) → caller's run loop dies → auto-restart
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
    this.metrics.recordHandlerRetry(originalTopic);

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

    // Check circuit breaker before sending to DLQ
    const circuitKey = `dlq:${options.topic}`;
    if (!this.circuitBreaker.canExecute(circuitKey)) {
      const circuitState = this.circuitBreaker.getState(circuitKey);
      this.logger.error(
        `Circuit breaker ${circuitState} for DLQ ${options.topic}, message dropped`,
      );
      this.metrics.recordFinalFailure(originalTopic, false);
      return { action: 'complete' };
    }

    try {
      await this.sendToDlq(
        message,
        error,
        options,
        originalTopic,
        attempt,
        connection,
      );
      this.circuitBreaker.recordSuccess(circuitKey);
      return { action: 'complete' };
    } catch (dlqError) {
      this.circuitBreaker.recordFailure(circuitKey);
      throw dlqError; // infra crash path — propagates out of the caller's retry loop
    }
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

      await this.kafkaClient.send(
        options.topic,
        {
          key: messageKey,
          value: messageValue,
          headers,
        },
        connection ? { connection } : undefined,
      );

      // Record metrics
      this.metrics.recordSentToDlq(originalTopic, options.topic);

      this.logger.warn(
        `Message sent to DLQ: ${options.topic} after ${retryCount} retries`,
      );
    } catch (dlqError) {
      this.logger.error('Failed to send message to DLQ', dlqError);
      throw dlqError;
    }
  }

  /** @deprecated Retry state is no longer held in memory; this is a no-op. */
  clearRetryState(): void {}

  /**
   * Get circuit breaker state for a DLQ topic
   */
  getCircuitState(dlqTopic: string): CircuitState {
    return this.circuitBreaker.getState(`dlq:${dlqTopic}`);
  }

  /**
   * Manually reset circuit breaker for a DLQ topic
   */
  resetCircuit(dlqTopic: string): void {
    this.circuitBreaker.reset(`dlq:${dlqTopic}`);
  }
}
