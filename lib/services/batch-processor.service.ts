import { Injectable, Logger } from '@nestjs/common';
import { KafkaMessage, EachBatchPayload } from 'kafkajs';
import {
  ConsumerOptions,
  GroupedBatch,
  KafkaMessagePayload,
} from '../interfaces';

interface BatchAccumulator {
  add: (message: KafkaMessage) => Promise<void>;
  onFlush: (callback: (messages: KafkaMessage[]) => Promise<void>) => void;
  flush: () => Promise<void>;
  size: () => number;
}

@Injectable()
export class BatchProcessorService {
  private readonly logger = new Logger(BatchProcessorService.name);

  createBatchAccumulator(options: ConsumerOptions): BatchAccumulator {
    const batchSize = options.batchSize || 100;
    const batchTimeout = options.batchTimeout || 5000;

    let buffer: KafkaMessage[] = [];
    let timer: NodeJS.Timeout | null = null;
    let flushCallback: ((messages: KafkaMessage[]) => Promise<void>) | null =
      null;

    const flush = async () => {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }

      if (buffer.length === 0) return;

      const messages = [...buffer];
      buffer = [];

      if (flushCallback) {
        await flushCallback(messages);
      }
    };

    const scheduleFlush = () => {
      if (timer) return;
      timer = setTimeout(() => {
        void flush();
      }, batchTimeout);
    };

    return {
      add: async (message: KafkaMessage) => {
        buffer.push(message);

        if (buffer.length >= batchSize) {
          await flush();
        } else {
          scheduleFlush();
        }
      },

      onFlush: (callback: (messages: KafkaMessage[]) => Promise<void>) => {
        flushCallback = callback;
      },

      flush,

      size: () => buffer.length,
    };
  }

  groupMessagesByKey<T>(messages: KafkaMessage[]): GroupedBatch<T>[] {
    const grouped = new Map<string, KafkaMessage[]>();

    for (const message of messages) {
      const key = message.key?.toString() || '__null_key__';

      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key)!.push(message);
    }

    return Array.from(grouped.entries()).map(([key, msgs]) => ({
      key,
      messages: msgs as unknown as KafkaMessagePayload<T>[],
    }));
  }

  async processBatch<T>(
    messages: KafkaMessage[],
    options: ConsumerOptions,
    handler: (messages: KafkaMessage[] | GroupedBatch<T>[]) => Promise<void>,
  ): Promise<void> {
    if (options.groupByKey) {
      const grouped = this.groupMessagesByKey<T>(messages);
      await handler(grouped);
    } else {
      await handler(messages);
    }
  }

  createEachBatchHandler(
    options: ConsumerOptions,
    handler: (messages: KafkaMessage[]) => Promise<void>,
  ) {
    const accumulator = this.createBatchAccumulator(options);

    accumulator.onFlush(async (messages) => {
      await this.processBatch(messages, options, handler);
    });

    const batchHandler = async (payload: EachBatchPayload): Promise<void> => {
      for (const message of payload.batch.messages) {
        if (!payload.isRunning() || payload.isStale()) break;

        await accumulator.add(message);
        payload.resolveOffset(message.offset);
        await payload.heartbeat();
      }

      await accumulator.flush();
    };

    return batchHandler;
  }
}
