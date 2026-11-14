/**
 * In-memory benchmark of the consume path: a real ConsumerRegistryService,
 * BatchProcessorService and IdempotencyService driven through the run config
 * captured from a fake consumer. No broker, no native binding (the `lib`
 * root's manual mock of the Kafka client applies). Run: `npm run bench`.
 */
import { Logger } from '@nestjs/common';
import { ConsumerRegistryService } from '../../lib/services/consumer-registry.service';
import { BatchProcessorService } from '../../lib/services/batch-processor.service';
import { IdempotencyService } from '../../lib/services/idempotency.service';

jest.mock('@confluentinc/kafka-javascript');

const MESSAGES = 100_000;
const WARMUP = 10_000;
const RUNS = 5;
const BATCH_FETCH = 500;

type ValueKind = 'json' | 'text';

const valueOf = (kind: ValueKind, n: number) =>
  Buffer.from(
    kind === 'json' ? `{"id":${n},"status":"ok"}` : `order ${n} processed`,
  );

const message = (kind: ValueKind, n: number) => ({
  offset: String(n),
  key: Buffer.from(`key-${n}`),
  value: valueOf(kind, n),
  headers: {},
  timestamp: '0',
});

async function startRegistry(options: Record<string, unknown>) {
  let runConfig: any;
  const consumer = {
    connect: async () => {},
    subscribe: async () => {},
    disconnect: async () => {},
    resume: () => {},
    commitOffsets: async () => {},
    run: async (config: any) => {
      runConfig = config;
    },
  };
  const core = {
    setConsumerStatus: () => {},
    disconnectAll: async () => {},
    getKafka: () => ({ consumer: () => consumer }),
  };
  const idempotency = new IdempotencyService({ idempotencyMaxKeys: 1_000_000 });
  const registry = new ConsumerRegistryService(
    core as any,
    new BatchProcessorService(),
    idempotency,
    {} as any,
    undefined,
    undefined,
  );
  let count = 0;
  const target = {
    handle: async (payload: unknown) => {
      count += Array.isArray(payload) ? payload.length : 1;
    },
  };
  registry.registerConsumers([
    {
      topic: 't',
      connection: 'default',
      options,
      target,
      methodName: 'handle',
    } as any,
  ]);
  await registry.startAll();
  return {
    runConfig,
    count: () => count,
    stop: () => idempotency.stopCleanup(),
  };
}

/** Feeds `total` messages starting at offset `from`; returns elapsed ns. */
async function feed(
  runConfig: any,
  batch: boolean,
  kind: ValueKind,
  from: number,
  total: number,
): Promise<bigint> {
  const start = process.hrtime.bigint();
  if (batch) {
    for (let i = from; i < from + total; i += BATCH_FETCH) {
      const messages: ReturnType<typeof message>[] = [];
      for (let n = i; n < Math.min(i + BATCH_FETCH, from + total); n++) {
        messages.push(message(kind, n));
      }
      await runConfig.eachBatch({
        batch: { topic: 't', partition: 0, messages },
        isRunning: () => true,
        isStale: () => false,
        resolveOffset: () => {},
        pause: () => () => {},
        heartbeat: async () => {},
        commitOffsetsIfNecessary: async () => {},
      });
    }
  } else {
    for (let n = from; n < from + total; n++) {
      await runConfig.eachMessage({
        topic: 't',
        partition: 0,
        message: message(kind, n),
        pause: () => () => {},
        heartbeat: async () => {},
      });
    }
  }
  return process.hrtime.bigint() - start;
}

const idempotencyKey = (m: { key: Buffer }) => m.key.toString();

const scenarios: Array<{
  name: string;
  options: Record<string, unknown>;
  kind: ValueKind;
}> = [
  { name: 'message/json', options: {}, kind: 'json' },
  { name: 'message/text', options: {}, kind: 'text' },
  { name: 'message/json+idem', options: { idempotencyKey }, kind: 'json' },
  {
    name: 'batch/json',
    options: { batch: true, batchSize: 100 },
    kind: 'json',
  },
  {
    name: 'batch/json+idem',
    options: { batch: true, batchSize: 100, idempotencyKey },
    kind: 'json',
  },
];

describe('consume path benchmark', () => {
  beforeAll(() => Logger.overrideLogger(['error']));

  for (const { name, options, kind } of scenarios) {
    it(name, async () => {
      const { runConfig, count, stop } = await startRegistry(options);
      const batch = options.batch === true;
      // Unique offsets and keys across warm-up and runs: idempotency never hits
      let offset = 0;
      await feed(runConfig, batch, kind, offset, WARMUP);
      offset += WARMUP;
      const times: number[] = [];
      for (let r = 0; r < RUNS; r++) {
        const ns = await feed(runConfig, batch, kind, offset, MESSAGES);
        offset += MESSAGES;
        times.push(Number(ns) / 1e6);
      }
      stop();
      expect(count()).toBe(WARMUP + RUNS * MESSAGES);

      times.sort((a, b) => a - b);
      const medianMs = times[Math.floor(RUNS / 2)];
      const perSecond = Math.round((MESSAGES / medianMs) * 1000);
      process.stdout.write(
        `BENCH ${name.padEnd(20)} ${String(perSecond).padStart(10)} msg/s ${medianMs.toFixed(1).padStart(9)} ms/100k\n`,
      );
    });
  }
});
