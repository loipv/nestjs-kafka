import { KafkaJS } from '@confluentinc/kafka-javascript';

type KafkaMessage = KafkaJS.KafkaMessage;

/**
 * Partition assignment strategies for consumer groups.
 * - roundRobin: Assigns partitions to consumers in a round-robin fashion
 * - range: Assigns partitions to consumers based on ranges
 * - cooperativeSticky: Cooperative rebalancing with sticky assignment (recommended for minimal rebalancing disruption)
 */
export type PartitionAssigner = 'roundrobin' | 'range' | 'cooperative-sticky';

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
  /** Exponential factor for internal retry backoff calculation. Default: 2 */
  multiplier?: number;
  /**
   * Skip message after max retries exceeded (for non-DLQ scenarios).
   * - true: skip message and continue (prevents consumer blocking)
   * - false: throw error after max retries (default, may cause consumer to stop)
   *
   * Note: This only applies when DLQ is NOT configured.
   * With DLQ, messages are sent to DLQ topic after max retries.
   */
  skipMessageOnMaxRetries?: boolean;
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
  // Note: autoCommitThreshold is NOT supported in confluent-kafka-javascript

  fromBeginning?: boolean;

  /** Allow auto creation of topic if it doesn't exist. Default: false */
  allowAutoTopicCreation?: boolean;

  /**
   * Partition assignment strategies for consumer group rebalancing.
   * - 'roundrobin': Assigns partitions in round-robin fashion
   * - 'range': Assigns partitions based on ranges (default)
   * - 'cooperative-sticky': Cooperative rebalancing with sticky assignment (recommended)
   *
   * Can specify multiple strategies; first one is primary.
   * @example ['cooperative-sticky'] or ['roundrobin', 'range']
   */
  partitionAssigners?: PartitionAssigner[];

  /** Retry options for consumer restart on failure */
  retry?: ConsumerRetryOptions;

  /** Partitions for auto-created topics. Default: 1 */
  autoCreateTopicPartitions?: number;

  /** Replication factor for auto-created topics. Default: 1 */
  autoCreateTopicReplicationFactor?: number;
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

/**
 * Default options for all consumers when not specified in @Consumer decorator.
 * These options will be merged with decorator options (decorator takes precedence).
 */
export interface ConsumerModuleOptions {
  /**
   * Default partition assignment strategies for consumer groups.
   * Applied when @Consumer decorator doesn't specify partitionAssigners.
   * @example ['cooperative-sticky']
   */
  partitionAssigners?: PartitionAssigner[];

  /**
   * Default setting for auto topic creation.
   * Applied when @Consumer decorator doesn't specify allowAutoTopicCreation.
   */
  allowAutoTopicCreation?: boolean;

  /**
   * Default session timeout in ms.
   * Applied when @Consumer decorator doesn't specify sessionTimeout.
   */
  sessionTimeout?: number;

  /**
   * Default heartbeat interval in ms.
   * Applied when @Consumer decorator doesn't specify heartbeatInterval.
   */
  heartbeatInterval?: number;

  /**
   * Default rebalance timeout in ms.
   * Applied when @Consumer decorator doesn't specify rebalanceTimeout.
   */
  rebalanceTimeout?: number;

  /**
   * Default auto commit setting.
   * Applied when @Consumer decorator doesn't specify autoCommit.
   */
  autoCommit?: boolean;

  /**
   * Default auto commit interval in ms.
   * Applied when @Consumer decorator doesn't specify autoCommitInterval.
   */
  autoCommitInterval?: number;

  /**
   * Default fromBeginning setting.
   * Applied when @Consumer decorator doesn't specify fromBeginning.
   */
  fromBeginning?: boolean;

  /**
   * Default retry options for consumers.
   * Applied when @Consumer decorator doesn't specify retry options.
   */
  retry?: ConsumerRetryOptions;

  /**
   * Default partition count for auto-created topics.
   * Applied when @Consumer decorator doesn't specify autoCreateTopicPartitions.
   */
  autoCreateTopicPartitions?: number;

  /**
   * Default replication factor for auto-created topics.
   * Applied when @Consumer decorator doesn't specify autoCreateTopicReplicationFactor.
   */
  autoCreateTopicReplicationFactor?: number;
}

/** Injection token for ConsumerModule options */
export const CONSUMER_MODULE_OPTIONS = Symbol('CONSUMER_MODULE_OPTIONS');

/** Hard cap for every computed retry delay (message retry and restart backoff) */
export const MAX_RETRY_DELAY_MS = 30000;

/**
 * What the retry engine should do after a handler failure.
 *
 * There is NO 'crash' verdict: handler errors retry in-process forever
 * (skip=false) or terminate via skip/DLQ. Infra failures (e.g. DLQ send
 * failure) REJECT instead of returning a verdict → run loop dies →
 * auto-restart.
 */
export type RetryVerdict =
  | { action: 'retry'; delayMs: number }
  /** ack: commit offset, move on (DLQ-sent / skipped / circuit-dropped) */
  | { action: 'complete' };
