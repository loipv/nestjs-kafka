import { Injectable, Logger } from '@nestjs/common';

/**
 * Service to track DLQ metrics and statistics
 */
@Injectable()
export class DlqMetricsService {
  private readonly logger = new Logger(DlqMetricsService.name);

  // Counters
  private handlerRetries = 0;
  private messagesSentToDlq = 0;
  private reprocessAttempts = 0;
  private reprocessSuccess = 0;
  private finalFailures = 0;

  // Per-topic metrics
  private topicMetrics = new Map<string, TopicMetrics>();

  /**
   * Record a handler retry attempt (before DLQ)
   */
  recordHandlerRetry(topic: string): void {
    this.handlerRetries++;
    this.getOrCreateTopicMetrics(topic).handlerRetries++;
  }

  /**
   * Record a message sent to DLQ
   */
  recordSentToDlq(topic: string, dlqTopic: string): void {
    this.messagesSentToDlq++;
    const metrics = this.getOrCreateTopicMetrics(topic);
    metrics.sentToDlq++;
    this.logger.debug(`Message sent to DLQ: ${topic} -> ${dlqTopic}`);
  }

  /**
   * Record a reprocess attempt from DLQ
   */
  recordReprocessAttempt(dlqTopic: string): void {
    this.reprocessAttempts++;
    this.getOrCreateTopicMetrics(dlqTopic).reprocessAttempts++;
  }

  /**
   * Record a successful reprocess from DLQ
   */
  recordReprocessSuccess(dlqTopic: string): void {
    this.reprocessSuccess++;
    this.getOrCreateTopicMetrics(dlqTopic).reprocessSuccess++;
  }

  /**
   * Record a final failure (sent to final DLQ or dropped)
   */
  recordFinalFailure(dlqTopic: string, sent: boolean): void {
    this.finalFailures++;
    const metrics = this.getOrCreateTopicMetrics(dlqTopic);
    if (sent) {
      metrics.sentToFinalDlq++;
    } else {
      metrics.dropped++;
    }
  }

  /**
   * Get all metrics
   */
  getMetrics(): DlqMetrics {
    const topicMetrics: Record<string, TopicMetrics> = {};
    for (const [topic, metrics] of this.topicMetrics) {
      topicMetrics[topic] = { ...metrics };
    }

    return {
      global: {
        handlerRetries: this.handlerRetries,
        messagesSentToDlq: this.messagesSentToDlq,
        reprocessAttempts: this.reprocessAttempts,
        reprocessSuccess: this.reprocessSuccess,
        reprocessFailureRate:
          this.reprocessAttempts > 0
            ? ((this.reprocessAttempts - this.reprocessSuccess) /
                this.reprocessAttempts) *
              100
            : 0,
        finalFailures: this.finalFailures,
      },
      byTopic: topicMetrics,
    };
  }

  /**
   * Reset all metrics
   */
  reset(): void {
    this.handlerRetries = 0;
    this.messagesSentToDlq = 0;
    this.reprocessAttempts = 0;
    this.reprocessSuccess = 0;
    this.finalFailures = 0;
    this.topicMetrics.clear();
  }

  private getOrCreateTopicMetrics(topic: string): TopicMetrics {
    let metrics = this.topicMetrics.get(topic);
    if (!metrics) {
      metrics = {
        handlerRetries: 0,
        sentToDlq: 0,
        reprocessAttempts: 0,
        reprocessSuccess: 0,
        sentToFinalDlq: 0,
        dropped: 0,
      };
      this.topicMetrics.set(topic, metrics);
    }
    return metrics;
  }
}

export interface TopicMetrics {
  handlerRetries: number;
  sentToDlq: number;
  reprocessAttempts: number;
  reprocessSuccess: number;
  sentToFinalDlq: number;
  dropped: number;
}

export interface DlqMetrics {
  global: {
    handlerRetries: number;
    messagesSentToDlq: number;
    reprocessAttempts: number;
    reprocessSuccess: number;
    reprocessFailureRate: number;
    finalFailures: number;
  };
  byTopic: Record<string, TopicMetrics>;
}
