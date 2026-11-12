import { Injectable } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { GroupedBatch, KafkaMessagePayload } from '../interfaces';

type KafkaMessage = KafkaJS.KafkaMessage;

const DEFAULT_BATCH_SIZE = 100;

@Injectable()
export class BatchProcessorService {
  /**
   * Split fetched messages into handler batches of at most `batchSize`.
   * A missing, zero or negative `batchSize` means 100.
   */
  splitIntoBatches<T>(messages: T[], batchSize?: number): T[][] {
    const size =
      batchSize !== undefined && batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
    const batches: T[][] = [];
    for (let i = 0; i < messages.length; i += size) {
      batches.push(messages.slice(i, i + size));
    }
    return batches;
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
}
