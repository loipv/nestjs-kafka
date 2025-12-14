import { KafkaMessage } from 'kafkajs';

export interface DlqOptions {
  topic: string;
  maxRetries?: number;
  retryDelay?: number;
  retryBackoffMultiplier?: number;
  includeOriginalHeaders?: boolean;
  includeErrorInfo?: boolean;
}

export interface ConsumerOptions {
  topic?: string;

  /** Skip this consumer when true (default: false) */
  disabled?: boolean;

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
}

export interface ConsumerMetadata {
  topic: string;
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
