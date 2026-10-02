import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';

type KafkaMessage = KafkaJS.KafkaMessage;

interface IdempotencyEntry {
  key: string;
  timestamp: number;
  ttl: number;
}

@Injectable()
export class IdempotencyService implements OnModuleDestroy {
  private processedKeys = new Map<string, IdempotencyEntry>();
  private cleanupInterval: NodeJS.Timeout | null = null;
  readonly defaultTtl = 3600000; // 1 hour
  private readonly CLEANUP_INTERVAL = 60000; // 60 seconds

  constructor() {
    this.startCleanup();
  }

  onModuleDestroy(): void {
    this.stopCleanup();
  }

  isProcessed(
    message: KafkaMessage,
    keyExtractor?: (msg: KafkaMessage) => string | undefined,
  ): boolean {
    const key = this.extractKey(message, keyExtractor);
    if (!key) return false;

    const entry = this.processedKeys.get(key);
    if (!entry) return false;

    // Check if entry has expired
    if (Date.now() - entry.timestamp > entry.ttl) {
      this.processedKeys.delete(key);
      return false;
    }

    return true;
  }

  markProcessed(
    message: KafkaMessage,
    keyExtractor?: (msg: KafkaMessage) => string | undefined,
    ttl?: number,
  ): void {
    const key = this.extractKey(message, keyExtractor);
    if (!key) return;

    this.processedKeys.set(key, {
      key,
      timestamp: Date.now(),
      ttl: ttl ?? this.defaultTtl,
    });
  }

  filterDuplicates(
    messages: KafkaMessage[],
    keyExtractor?: (msg: KafkaMessage) => string | undefined,
  ): KafkaMessage[] {
    return messages.filter((msg) => !this.isProcessed(msg, keyExtractor));
  }

  private extractKey(
    message: KafkaMessage,
    keyExtractor?: (msg: KafkaMessage) => string | undefined,
  ): string | undefined {
    if (keyExtractor) {
      return keyExtractor(message);
    }

    const header = message.headers?.['idempotency-key'];
    if (header) {
      return Buffer.isBuffer(header) ? header.toString() : String(header);
    }

    return undefined;
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
