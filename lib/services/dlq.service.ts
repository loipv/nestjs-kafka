import { Injectable, Logger, forwardRef, Inject } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaClient } from './kafka-client.service';

type KafkaMessage = KafkaJS.KafkaMessage;
type IHeaders = KafkaJS.IHeaders;
import { DlqOptions } from '../interfaces';
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
    private readonly metrics: DlqMetricsService,
    private readonly circuitBreaker: CircuitBreakerService,
  ) {}

  async handleFailure(
    message: KafkaMessage,
    error: Error,
    options: DlqOptions,
    originalTopic: string,
    partition?: number,
    connection?: string,
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

    // Record retry attempt
    this.metrics.recordHandlerRetry(originalTopic);

    if (state.retryCount <= maxRetries) {
      const baseDelay = options.retryDelay ?? 1000;
      const multiplier = options.retryBackoffMultiplier ?? 2;
      const delay = baseDelay * Math.pow(multiplier, state.retryCount - 1);

      this.logger.warn(
        `Retry ${state.retryCount}/${maxRetries} for message from ${originalTopic}, waiting ${delay}ms`,
      );

      await this.sleep(delay);
      return true;
    }

    // Check circuit breaker before sending to DLQ
    const circuitKey = `dlq:${options.topic}`;
    if (!this.circuitBreaker.canExecute(circuitKey)) {
      const circuitState = this.circuitBreaker.getState(circuitKey);
      this.logger.error(
        `Circuit breaker ${circuitState} for DLQ ${options.topic}, message dropped`,
      );
      this.retryStates.delete(messageKey);
      this.metrics.recordFinalFailure(originalTopic, false);
      return false;
    }

    try {
      await this.sendToDlq(
        message,
        error,
        options,
        originalTopic,
        state.retryCount,
        connection,
      );
      this.circuitBreaker.recordSuccess(circuitKey);
    } catch (dlqError) {
      this.circuitBreaker.recordFailure(circuitKey);
      throw dlqError;
    }

    this.retryStates.delete(messageKey);
    return false;
  }

  private async sendToDlq(
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
