import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';

type KafkaMessage = KafkaJS.KafkaMessage;

interface IdempotencyEntry {
  key: string;
  timestamp: number;
}

@Injectable()
export class IdempotencyService implements OnModuleDestroy {
  private readonly logger = new Logger(IdempotencyService.name);

  private processedKeys = new Map<string, IdempotencyEntry>();
  private cleanupInterval: NodeJS.Timeout | null = null;
  private readonly defaultTtl = 3600000;

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

    return this.processedKeys.has(key);
  }

  markProcessed(
    message: KafkaMessage,
    keyExtractor?: (msg: KafkaMessage) => string | undefined,
  ): void {
    const key = this.extractKey(message, keyExtractor);
    if (!key) return;

    this.processedKeys.set(key, {
      key,
      timestamp: Date.now(),
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
      const now = Date.now();

      for (const [key, entry] of this.processedKeys.entries()) {
        if (now - entry.timestamp > this.defaultTtl) {
          this.processedKeys.delete(key);
        }
      }
    }, 60000);
  }

  stopCleanup(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
  }
}
