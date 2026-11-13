import { KafkaJS } from '@confluentinc/kafka-javascript';

type KafkaMessage = KafkaJS.KafkaMessage;

/**
 * Partition assignment strategies for consumer groups.
 * - roundRobin: Assigns partitions to consumers in a round-robin fashion
 * - range: Assigns partitions to consumers based on ranges
 * - cooperativeSticky: Cooperative rebalancing with sticky assignment (recommended for minimal rebalancing disruption)
 */
export type PartitionAssigner = 'roundrobin' | 'range' | 'cooperative-sticky';

export interface DlqOptions {
  /** DLQ topic to send failed messages */
  topic: string;
  /**
   * Connection name to produce the DLQ on (any registered connection).
   * Default: the consumer's connection
   */
  connection?: string;
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
  /**
   * Max time for a DLQ send in ms; a timeout counts as a failed send and the
   * message is retried. At most 2147483647. Default: 30000
   */
  sendTimeout?: number;
}

export interface ConsumerRetryOptions {
  /**
   * Attempts before `skipMessageOnMaxRetries` applies. Without skip the
   * message is retried indefinitely. Default: 3
   */
  retries?: number;
  /**
   * Cap for every retry delay in ms (also caps the DLQ backoff).
   * At most 2147483647. Default: 30000
   */
  maxRetryTime?: number;
  /** Delay before the first retry in ms. At most 2147483647. Default: 1000 */
  initialRetryTime?: number;
  /** Exponential factor for the retry delay. Default: 2 */
  multiplier?: number;
  /**
   * Skip message after max retries exceeded (for non-DLQ scenarios).
   * - true: skip the message and commit its offset
   * - false (default): keep retrying; the partition is paused between
   *   attempts, other partitions keep flowing
   *
   * Note: This only applies when DLQ is NOT configured.
   * With DLQ, messages are sent to DLQ topic after `dlq.maxRetries`.
   */
  skipMessageOnMaxRetries?: boolean;
}

export interface ConsumerOptions {
  /** Connection name for multi-connection support. Default: 'default' */
  connection?: string;

  /** Skip this consumer when true (default: false) */
  disabled?: boolean;

  /** Auto-deserialize message value (JSON parse or string). Default: true */
  deserialize?: boolean;

  /** Consumer group. Default: `${topic}-group` */
  groupId?: string;
  /** Group session timeout in ms. Default: the client's (librdkafka `session.timeout.ms`) */
  sessionTimeout?: number;
  /** Heartbeat interval in ms. Default: the client's (librdkafka `heartbeat.interval.ms`) */
  heartbeatInterval?: number;
  /** Rebalance timeout in ms. Default: the client's */
  rebalanceTimeout?: number;

  /** Deliver messages to the handler in arrays (batch consumer). Default: false */
  batch?: boolean;
  /** Max messages per handler call (batch consumers). Default: 100 */
  batchSize?: number;

  /** Batch consumers: group each chunk by message key. Default: false */
  groupByKey?: boolean;

  /** Partitions processed in parallel by this consumer. Default: 1 */
  partitionsConsumedConcurrently?: number;

  /** Extracts the key that identifies a duplicate; enables idempotency */
  idempotencyKey?: (message: KafkaMessage) => string | undefined;
  /** How long a processed key is remembered, in ms. Default: 3600000 */
  idempotencyTtl?: number;

  dlq?: DlqOptions;

  /**
   * Commit offsets in the background every `autoCommitInterval`. When false,
   * the library commits synchronously after each handled message (or batch
   * chunk): fewer redeliveries after a crash, lower throughput. Default: true
   */
  autoCommit?: boolean;
  /** Auto-commit interval in ms. Default: the client's (librdkafka `auto.commit.interval.ms`) */
  autoCommitInterval?: number;
  // Note: autoCommitThreshold is NOT supported in confluent-kafka-javascript

  /** Start from the earliest offset when the group has no committed offset. Default: false */
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

  /**
   * Retry of a failing handler: the partition is paused and the message
   * redelivered after a capped exponential backoff (see skipMessageOnMaxRetries)
   */
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

/**
 * Default options for all consumers when not specified in @Consumer decorator.
 * These options will be merged with decorator options (decorator takes precedence).
 */
export interface ConsumerModuleOptions {
  /**
   * Max idempotency keys kept in memory; the oldest are evicted.
   * Default: 100000
   */
  idempotencyMaxKeys?: number;

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

/** Default cap for every retry delay (override per consumer with `retry.maxRetryTime`) */
export const MAX_RETRY_DELAY_MS = 30000;

/**
 * What the retry engine should do after a handler failure.
 *
 * There is NO 'crash' verdict: handler errors are retried forever (skip=false)
 * or end via skip/DLQ. A retry pauses the partition, rethrows, and resumes
 * after `delayMs`; a failed DLQ send is a retry too.
 */
export type RetryVerdict =
  | { action: 'retry'; delayMs: number }
  /** ack: commit offset, move on (DLQ-sent / skipped) */
  | { action: 'complete' };
