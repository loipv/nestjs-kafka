import { Inject, Injectable, OnModuleDestroy, Optional } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { CONSUMER_MODULE_OPTIONS, ConsumerModuleOptions } from '../interfaces';

type KafkaMessage = KafkaJS.KafkaMessage;

interface IdempotencyEntry {
  timestamp: number;
  ttl: number;
}

@Injectable()
export class IdempotencyService implements OnModuleDestroy {
  private processedKeys = new Map<string, IdempotencyEntry>();
  private cleanupInterval: NodeJS.Timeout | null = null;
  readonly defaultTtl = 3600000; // 1 hour
  private readonly CLEANUP_INTERVAL = 60000; // 60 seconds
  /** Oldest keys are evicted beyond this many entries */
  private readonly maxKeys: number;

  constructor(
    @Optional()
    @Inject(CONSUMER_MODULE_OPTIONS)
    moduleOptions?: ConsumerModuleOptions,
  ) {
    const maxKeys = moduleOptions?.idempotencyMaxKeys ?? 100000;
    if (!Number.isInteger(maxKeys) || maxKeys <= 0) {
      throw new Error('idempotencyMaxKeys must be a positive integer');
    }
    this.maxKeys = maxKeys;
    this.startCleanup();
  }

  onModuleDestroy(): void {
    this.stopCleanup();
  }

  /**
   * The stored key for `message`: `<namespace>|<extracted>`, or undefined when
   * the extractor returns nothing (never deduplicated). `namespace` scopes
   * keys (the registry passes `<connection>:<groupId>|<topic>`) so two
   * consumers extracting the same key never dedupe each other. Extractor
   * errors propagate.
   */
  keyOf(
    message: KafkaMessage,
    keyExtractor: (msg: KafkaMessage) => string | undefined,
    namespace: string,
  ): string | undefined {
    const extracted = keyExtractor(message);
    return extracted ? `${namespace}|${extracted}` : undefined;
  }

  /** Whether `key` (from keyOf) is stored and unexpired; drops an expired one. */
  has(key: string): boolean {
    const entry = this.processedKeys.get(key);
    if (!entry) return false;

    if (Date.now() - entry.timestamp > entry.ttl) {
      this.processedKeys.delete(key);
      return false;
    }

    return true;
  }

  /** Stores `key` (from keyOf) as processed; evicts the oldest beyond the cap. */
  mark(key: string, ttl?: number): void {
    // delete + set moves the key to the end of the Map's insertion order
    this.processedKeys.delete(key);
    this.processedKeys.set(key, {
      timestamp: Date.now(),
      ttl: ttl ?? this.defaultTtl,
    });

    while (this.processedKeys.size > this.maxKeys) {
      const oldest = this.processedKeys.keys().next().value as string;
      this.processedKeys.delete(oldest);
    }
  }

  isProcessed(
    message: KafkaMessage,
    keyExtractor: (msg: KafkaMessage) => string | undefined,
    namespace: string,
  ): boolean {
    const key = this.keyOf(message, keyExtractor, namespace);
    return key !== undefined && this.has(key);
  }

  markProcessed(
    message: KafkaMessage,
    keyExtractor: (msg: KafkaMessage) => string | undefined,
    namespace: string,
    ttl?: number,
  ): void {
    const key = this.keyOf(message, keyExtractor, namespace);
    if (key !== undefined) this.mark(key, ttl);
  }

  filterDuplicates(
    messages: KafkaMessage[],
    keyExtractor: (msg: KafkaMessage) => string | undefined,
    namespace: string,
  ): KafkaMessage[] {
    return messages.filter(
      (msg) => !this.isProcessed(msg, keyExtractor, namespace),
    );
  }

  private startCleanup(): void {
    this.cleanupInterval = setInterval(() => {
      this.runCleanupBatch();
    }, this.CLEANUP_INTERVAL);
    // Housekeeping only — must not keep the process alive if shutdown hooks
    // never run (e.g. app init failed before close()).
    this.cleanupInterval.unref();
  }

  private runCleanupBatch(): void {
    const now = Date.now();
    for (const [key, entry] of this.processedKeys) {
      if (now - entry.timestamp > entry.ttl) {
        this.processedKeys.delete(key);
      }
    }
  }

  stopCleanup(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
  }
}
