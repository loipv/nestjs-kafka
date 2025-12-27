import { KafkaMessage } from 'kafkajs';

/**
 * Options for DLQ retry - auto consume from DLQ and retry handler
 */
export interface DlqRetryOptions {
  /** Enable DLQ retry consumer. Default: false */
  enabled?: boolean;
  /** Max retries from DLQ before sending to final dead letter. Default: 3 */
  maxRetries?: number;
  /** Delay before retrying in ms. Default: 60000 (1 minute) */
  delay?: number;
  /** Backoff multiplier for retry delay. Default: 2 */
  backoffMultiplier?: number;
  /** Final dead letter topic. If not set, messages are dropped after max retries */
  finalDlqTopic?: string;
  /** GroupId for DLQ consumer. Default: ${dlqTopic}-retry-consumer */
  groupId?: string;
  /** Start consuming from beginning of DLQ topic. Default: false */
  fromBeginning?: boolean;
}

export interface DlqOptions {
  /** DLQ topic to send failed messages */
  topic: string;
  /** Max retries before sending to DLQ. Default: 3 */
  maxRetries?: number;
  /** Delay between retries in ms. Default: 1000 */
  retryDelay?: number;
  /** Backoff multiplier for retry delay. Default: 2 */
  retryBackoffMultiplier?: number;
  /** Include original message headers. Default: true */
  includeOriginalHeaders?: boolean;
  /** Include error info in headers. Default: true */
  includeErrorInfo?: boolean;
  /** Enable auto-consume from DLQ and retry handler */
  retry?: DlqRetryOptions;
}

export interface ConsumerRetryOptions {
  /** Maximum number of retries per call. Default: 5 */
  retries?: number;
  /** Max wait time for a retry in ms. Default: 30000 */
  maxRetryTime?: number;
  /** Initial value used to calculate retry in ms. Default: 300 */
  initialRetryTime?: number;
  /** Randomization factor. Default: 0.2 */
  factor?: number;
  /** Exponential factor. Default: 2 */
  multiplier?: number;
  /**
   * Control whether to restart consumer on failure.
   * - true: always restart (default)
   * - false: never restart
   * - function: custom logic to decide
   */
  restartOnFailure?: boolean | ((error: Error) => Promise<boolean>);
}

export interface ConsumerOptions {
  topic?: string;

  /** Connection name for multi-connection support. Default: 'default' */
  connection?: string;

  /** Skip this consumer when true (default: false) */
  disabled?: boolean;

  /** Auto-deserialize message value (JSON parse or string). Default: true */
  deserialize?: boolean;

  groupId?: string;
  sessionTimeout?: number;
  heartbeatInterval?: number;
  rebalanceTimeout?: number;

  batch?: boolean;
  batchSize?: number;
  batchTimeout?: number;

  groupByKey?: boolean;

  maxConcurrency?: number;
  partitionsConsumedConcurrently?: number;

  backPressureThreshold?: number;
  maxQueueSize?: number;

  idempotencyKey?: (message: KafkaMessage) => string | undefined;
  idempotencyTtl?: number;

  dlq?: DlqOptions;

  autoCommit?: boolean;
  autoCommitInterval?: number;
  autoCommitThreshold?: number;

  fromBeginning?: boolean;

  /** Allow auto creation of topic if it doesn't exist. Default: false */
  allowAutoTopicCreation?: boolean;

  /** Retry options for consumer restart on failure */
  retry?: ConsumerRetryOptions;
}

export interface ConsumerMetadata {
  topic: string;
  connection: string;
  options: ConsumerOptions;
  target: any;
  methodName: string;
}

export interface PressureState {
  isPaused: boolean;
  currentQueueSize: number;
  maxQueueSize: number;
  currentConcurrency: number;
  maxConcurrency: number;
  utilizationPercent: number;
}

export interface PressureManagerOptions {
  backPressureThreshold: number;
  resumeThreshold: number;
  maxQueueSize: number;
  checkIntervalMs?: number;
}
