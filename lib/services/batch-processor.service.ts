import { Injectable, Logger } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  ConsumerOptions,
  GroupedBatch,
  KafkaMessagePayload,
} from '../interfaces';

type KafkaMessage = KafkaJS.KafkaMessage;
type EachBatchPayload = KafkaJS.EachBatchPayload;

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

  groupMessagesByKey<T>(
    messages: Array<KafkaMessage | KafkaMessagePayload<T>>,
  ): GroupedBatch<T>[] {
    const grouped = new Map<
      string,
      Array<KafkaMessage | KafkaMessagePayload<T>>
    >();

    for (const message of messages) {
      const key = message.key?.toString() || '__null_key__';

      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key)!.push(message);
    }

    return Array.from(grouped.entries()).map(([key, msgs]) => ({
      key,
      messages: msgs as KafkaMessagePayload<T>[],
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
    handler: (
      messages: KafkaMessage[],
      topic: string,
      partition: number,
    ) => Promise<void>,
  ) {
    const batchHandler = async (payload: EachBatchPayload): Promise<void> => {
      const { topic, partition } = payload.batch;
      const accumulator = this.createBatchAccumulator(options);

      accumulator.onFlush(async (messages) => {
        await handler(messages, topic, partition);
      });

      for (const message of payload.batch.messages) {
        if (!payload.isRunning() || payload.isStale()) break;

        await accumulator.add(message);
        payload.resolveOffset(message.offset);
        // Note: heartbeat() is automatic in confluent-kafka-javascript
      }

      await accumulator.flush();
    };

    return batchHandler;
  }
}
