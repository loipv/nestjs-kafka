import { KafkaMessage } from 'kafkajs';

export interface DlqOptions {
  topic: string;
  maxRetries?: number;
  retryDelay?: number;
  retryBackoffMultiplier?: number;
  includeOriginalHeaders?: boolean;
  includeErrorInfo?: boolean;
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
  checkIntervalMs: number;
}
