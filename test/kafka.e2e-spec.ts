import { GenericContainer, Wait } from 'testcontainers';
import { Test } from '@nestjs/testing';
import { Injectable } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  KafkaModule,
  ConsumerModule,
  Consumer,
  KafkaClient,
  KafkaHealthIndicator,
  ConsumerRegistryService,
} from '../lib';

let kafka: Awaited<ReturnType<GenericContainer['start']>>;
let bootstrap: string;

// Self-contained single-node KRaft Kafka with a FIXED host port and the
// advertised listener pointing at 127.0.0.1:<port> — no mapped/advertised
// port mismatch possible. 127.0.0.1 (not "localhost") because librdkafka
// resolves localhost to IPv6 ::1, where Docker's published ports don't listen.
function startKafka(port: number, autoCreateTopics = true) {
  return new GenericContainer('confluentinc/cp-kafka:7.6.0')
    .withEnvironment({
      CLUSTER_ID: '5L6g3nShT-eMCtK--X86sw',
      KAFKA_NODE_ID: '1',
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093',
      KAFKA_LISTENERS: `PLAINTEXT://0.0.0.0:${port},CONTROLLER://0.0.0.0:9093`,
      KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://127.0.0.1:${port}`,
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
        'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: String(autoCreateTopics),
    })
    .withExposedPorts({ container: port, host: port })
    .withWaitStrategy(Wait.forLogMessage('Kafka Server started'))
    .start();
}

beforeAll(async () => {
  kafka = await startKafka(9092);
  bootstrap = '127.0.0.1:9092';
}, 180_000);

afterAll(async () => {
  await kafka?.stop();
});

// 45s default: the FIRST consumer group on a cold broker must wait for
// __consumer_offsets creation + coordinator election, which can exceed 20s.
async function waitFor(cond: () => boolean, timeout = 45_000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeout) throw new Error('waitFor timeout');
    await new Promise((r) => setTimeout(r, 100));
  }
}

async function createApp(
  providers: any[],
  rdkafka?: Record<string, string | number | boolean>,
) {
  const moduleRef = await Test.createTestingModule({
    imports: [
      KafkaModule.forRoot({ clientId: 'e2e', brokers: [bootstrap], rdkafka }),
      ConsumerModule.forRoot(),
    ],
    providers,
  }).compile();
  await moduleRef.init(); // triggers discovery + consumer connect
  return moduleRef;
}

describe('Kafka e2e (testcontainers)', () => {
  it('round-trips JSON and Buffer payloads intact (regression: fix #1)', async () => {
    const received: any[] = [];

    @Injectable()
    class RoundTripConsumer {
      @Consumer('e2e-roundtrip', {
        groupId: 'e2e-rt',
        fromBeginning: true,
        allowAutoTopicCreation: true,
      })
      async handle(msg: any) {
        received.push(msg);
      }
    }

    const app = await createApp([RoundTripConsumer]);
    try {
      const client = app.get(KafkaClient);
      await client.send('e2e-roundtrip', { value: { hello: 'world', n: 42 } });
      await client.send('e2e-roundtrip', {
        key: 'bin',
        value: Buffer.from('raw-bytes'),
      });

      await waitFor(() => received.length >= 2);
      expect(received[0].value).toEqual({ hello: 'world', n: 42 });
      const binMsg = received.find((m) => m.key === 'bin')!;
      expect(binMsg.value).toBe('raw-bytes'); // corrupted build would yield {type:'Buffer',data:[...]}
    } finally {
      await app.close();
    }
  });

  it('boots and round-trips with the documented producer options', async () => {
    const received: any[] = [];

    @Injectable()
    class ProducerOptionsConsumer {
      @Consumer('e2e-producer-opts', {
        groupId: 'e2e-po-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
      })
      async handle(msg: any) {
        received.push(msg);
      }
    }

    const app = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          clientId: 'e2e',
          brokers: [bootstrap],
          producer: { acks: -1, timeout: 30000, compression: 1 },
        }),
        ConsumerModule.forRoot(),
      ],
      providers: [ProducerOptionsConsumer],
    }).compile();
    await app.init();
    try {
      await app.get(KafkaClient).send('e2e-producer-opts', {
        value: { id: 'p' },
      });
      await waitFor(() => received.length >= 1);
      expect(received[0].value).toEqual({ id: 'p' });
    } finally {
      await app.close();
    }
  });

  it('retries a failing handler by redelivery, then keeps consuming (regression: fix #4)', async () => {
    let poisonAttempts = 0;
    const successes: string[] = [];

    @Injectable()
    class RetryConsumer {
      @Consumer('e2e-retry', {
        groupId: 'e2e-retry-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        retry: { retries: 2, initialRetryTime: 100, multiplier: 1 },
      })
      async handle(msg: any) {
        if (msg.value?.id === 'poison') {
          poisonAttempts++;
          if (poisonAttempts < 3) throw new Error('transient');
          return; // succeeds on 3rd attempt
        }
        successes.push(msg.value?.id);
      }
    }

    const app = await createApp([RetryConsumer]);
    try {
      const client = app.get(KafkaClient);
      await client.send('e2e-retry', { value: { id: 'poison' } });
      await client.send('e2e-retry', { value: { id: 'ok' } });

      await waitFor(() => poisonAttempts >= 3);
      await waitFor(() => successes.includes('ok'));
      expect(poisonAttempts).toBe(3);
    } finally {
      await app.close();
    }
  });

  it('dead-letters after max retries and keeps consuming (DLQ round-trip)', async () => {
    let attempts = 0;
    const successes: string[] = [];
    const dlqReceived: any[] = [];

    @Injectable()
    class SourceConsumer {
      @Consumer('e2e-dlq-src', {
        groupId: 'e2e-dlq-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        retry: { retries: 2, initialRetryTime: 100 },
        dlq: { topic: 'e2e-dlq-dead', maxRetries: 2, retryDelay: 100 },
      })
      async handle(msg: any) {
        if (msg.value?.fail) {
          attempts++;
          throw new Error('permanent');
        }
        successes.push(msg.value?.id);
      }
    }

    @Injectable()
    class DlqSpyConsumer {
      @Consumer('e2e-dlq-dead', {
        groupId: 'e2e-dlq-spy',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        deserialize: false,
      })
      async handle(msg: any) {
        dlqReceived.push(msg);
      }
    }

    const app = await createApp([SourceConsumer, DlqSpyConsumer]);
    try {
      const client = app.get(KafkaClient);
      await client.send('e2e-dlq-src', { value: { id: 'x', fail: true } });
      await client.send('e2e-dlq-src', { value: { id: 'good' } });

      await waitFor(() => dlqReceived.length >= 1);
      await waitFor(() => successes.includes('good'));
      expect(dlqReceived[0].headers['x-dlq-original-topic']?.toString()).toBe(
        'e2e-dlq-src',
      );
      expect(attempts).toBe(3); // 1 initial + 2 DLQ retries
    } finally {
      await app.close();
    }
  });

  it('delivers batched messages together with no loss (regression: fix #2)', async () => {
    const batches: any[][] = [];

    @Injectable()
    class BatchConsumer {
      @Consumer('e2e-batch', {
        groupId: 'e2e-batch-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        batch: true,
        batchSize: 3,
      })
      async handle(msgs: any[]) {
        batches.push(msgs);
      }
    }

    const app = await createApp([BatchConsumer]);
    try {
      const client = app.get(KafkaClient);
      for (let i = 0; i < 5; i++) {
        await client.send('e2e-batch', { value: { i } });
      }

      await waitFor(() => batches.flat().length >= 5);
      expect(
        batches
          .flat()
          .map((m: any) => m.value.i)
          .sort(),
      ).toEqual([0, 1, 2, 3, 4]);
    } finally {
      await app.close();
    }
  });

  it('dedupes messages sharing an idempotency key', async () => {
    const handled: string[] = [];

    @Injectable()
    class IdemConsumer {
      @Consumer('e2e-idem', {
        groupId: 'e2e-idem-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        idempotencyKey: (m: any) => m.headers?.['idempotency-key']?.toString(),
      })
      async handle(msg: any) {
        handled.push(msg.value.id);
      }
    }

    const app = await createApp([IdemConsumer]);
    try {
      const client = app.get(KafkaClient);
      await client.send('e2e-idem', {
        value: { id: 'a' },
        headers: { 'idempotency-key': 'dup' },
      });
      await client.send('e2e-idem', {
        value: { id: 'b' },
        headers: { 'idempotency-key': 'dup' },
      });
      await client.send('e2e-idem', {
        value: { id: 'c' },
        headers: { 'idempotency-key': 'uniq' },
      });

      await waitFor(() => handled.length >= 2);
      expect(handled.sort()).toEqual(['a', 'c']); // 'b' deduped
    } finally {
      await app.close();
    }
  });

  it('app.close() completes promptly with a long resume delay pending (regression: fix #3)', async () => {
    let attempts = 0;

    @Injectable()
    class SlowRetryConsumer {
      @Consumer('e2e-slow-retry', {
        groupId: 'e2e-slow-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        retry: { retries: 50, initialRetryTime: 60_000 },
      })
      async handle() {
        attempts++;
        throw new Error('slow poison');
      }
    }

    const app = await createApp([SlowRetryConsumer]);
    try {
      const client = app.get(KafkaClient);
      await client.send('e2e-slow-retry', { value: { x: 1 } });
      await waitFor(() => attempts >= 1);

      const t0 = Date.now();
      await app.close();
      expect(Date.now() - t0).toBeLessThan(10_000); // not blocked by the 60s delay
    } finally {
      await app.close().catch(() => undefined); // already closed on the happy path
    }
  });

  it('message mid-retry at shutdown is redelivered on next boot (no ack on abort)', async () => {
    let attempts = 0;
    const app1Successes: string[] = [];

    @Injectable()
    class MidRetryConsumer {
      @Consumer('e2e-midretry', {
        groupId: 'e2e-mr-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        retry: { retries: 50, initialRetryTime: 60_000 },
        // skip=false → retried forever; the partition is paused between
        // attempts, so shutdown never commits the poison offset.
      })
      async handle(msg: any) {
        if (msg.value?.id === 'poison') {
          attempts++;
          throw new Error('slow poison');
        }
        app1Successes.push(msg.value?.id);
      }
    }

    const app1 = await createApp([MidRetryConsumer]);
    let client: KafkaClient;
    try {
      client = app1.get(KafkaClient);
      await client.send('e2e-midretry', { value: { id: 'poison' } });
      await client.send('e2e-midretry', { value: { id: 'good' } });

      await waitFor(() => attempts >= 1); // poison's partition is paused for 60s
      expect(app1Successes).toEqual([]); // 'good' not reached yet (partition order)
    } finally {
      await app1.close(); // drops the pending 60s resume
    }

    // Next boot, same groupId: committed offset never advanced past 'poison'
    const redelivered: string[] = [];

    @Injectable()
    class RecoveryConsumer {
      @Consumer('e2e-midretry', {
        groupId: 'e2e-mr-g',
        allowAutoTopicCreation: true,
      })
      async handle(msg: any) {
        redelivered.push(msg.value?.id);
      }
    }

    const app2 = await createApp([RecoveryConsumer]);
    try {
      await waitFor(() => redelivered.length >= 2);
      expect(redelivered).toEqual(['poison', 'good']); // nothing lost, order preserved
    } finally {
      await app2.close();
    }
  });

  it('autoCommit false commits: a restarted group does not re-read handled messages', async () => {
    const first: string[] = [];

    @Injectable()
    class ManualCommitConsumer {
      @Consumer('e2e-autocommit', {
        groupId: 'e2e-ac-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        autoCommit: false,
      })
      async handle(msg: any) {
        first.push(msg.value?.id);
      }
    }

    const app1 = await createApp([ManualCommitConsumer]);
    try {
      const client = app1.get(KafkaClient);
      await client.send('e2e-autocommit', { value: { id: 'm1' } });
      await client.send('e2e-autocommit', { value: { id: 'm2' } });
      await waitFor(() => first.length >= 2);
    } finally {
      await app1.close();
    }

    const second: string[] = [];

    @Injectable()
    class RestartedConsumer {
      @Consumer('e2e-autocommit', {
        groupId: 'e2e-ac-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        autoCommit: false,
      })
      async handle(msg: any) {
        second.push(msg.value?.id);
      }
    }

    const app2 = await createApp([RestartedConsumer]);
    try {
      await app2
        .get(KafkaClient)
        .send('e2e-autocommit', { value: { id: 'marker' } });
      await waitFor(() => second.includes('marker'));
      await new Promise((r) => setTimeout(r, 3000));
      expect(second).toEqual(['marker']);
    } finally {
      await app2.close();
    }
  });

  it('retries past maxRetries indefinitely (skip=false) — consumer never crashes', async () => {
    let attempts = 0;
    const successes: string[] = [];

    @Injectable()
    class InfiniteRetryConsumer {
      @Consumer('e2e-infinite', {
        groupId: 'e2e-inf-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        retry: { retries: 2, initialRetryTime: 100, multiplier: 1 },
        // skip=false (default): retry continues PAST maxRetries=2 until success
      })
      async handle(msg: any) {
        if (msg.value?.id === 'poison' && attempts < 5) {
          attempts++;
          throw new Error('flaky');
        }
        successes.push(msg.value?.id);
      }
    }

    const app = await createApp([InfiniteRetryConsumer]);
    try {
      const client = app.get(KafkaClient);
      await client.send('e2e-infinite', { value: { id: 'poison' } });
      await client.send('e2e-infinite', { value: { id: 'good' } });

      // 3 attempts BEYOND maxRetries=2 — redelivered, no consumer restart, app stays up
      await waitFor(
        () => successes.includes('poison') && successes.includes('good'),
        60_000,
      );
      expect(attempts).toBe(5);
    } finally {
      await app.close();
    }
  });

  it('a retrying partition does not block another partition or get the member evicted', async () => {
    const t0 = Date.now();
    let firstFailureAt = 0;
    let aSuccesses = 0;
    let aSucceededAt = 0;
    const bSeen: string[] = [];
    const bDoneAt: number[] = [];

    @Injectable()
    class TwoPartitionConsumer {
      @Consumer('e2e-nonblocking', {
        groupId: 'e2e-nb-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        autoCreateTopicPartitions: 2,
        sessionTimeout: 6000,
        retry: { initialRetryTime: 1000, maxRetryTime: 2000 },
      })
      async handle(msg: any) {
        const id = msg.value?.id as string;
        if (id === 'A') {
          firstFailureAt ||= Date.now();
          if (Date.now() - firstFailureAt < 25_000)
            throw new Error('A not yet');
          aSuccesses++;
          aSucceededAt = Date.now();
          return;
        }
        bSeen.push(id);
        bDoneAt.push(Date.now());
      }
    }

    // librdkafka requires max.poll.interval.ms >= session.timeout.ms
    const app = await createApp([TwoPartitionConsumer], {
      'max.poll.interval.ms': 10000,
    });
    try {
      const client = app.get(KafkaClient);
      await client.send('e2e-nonblocking', {
        value: { id: 'A' },
        partition: 0,
      });
      await waitFor(() => firstFailureAt > 0);
      await new Promise((r) => setTimeout(r, 2000));
      for (let i = 1; i <= 5; i++) {
        await client.send('e2e-nonblocking', {
          value: { id: `B${i}` },
          partition: 1,
        });
      }

      await waitFor(() => aSuccesses >= 1, 60_000);
      await new Promise((r) => setTimeout(r, 3000)); // room for a duplicate to show up
      expect(bSeen.sort()).toEqual(['B1', 'B2', 'B3', 'B4', 'B5']);
      expect(Math.max(...bDoneAt)).toBeLessThan(aSucceededAt);
      expect(aSuccesses).toBe(1);
      expect(Date.now() - t0).toBeGreaterThan(25_000);
    } finally {
      await app.close();
    }
  }, 120_000);

  it('rebalance during a retry backoff resumes the partition', async () => {
    let failures = 0;
    let successes = 0;
    const handle = (msg: any) => {
      if (msg.value?.id !== 'A') return;
      if (failures === 0) {
        failures++;
        throw new Error('first delivery fails');
      }
      successes++;
    };
    const options = {
      groupId: 'e2e-rb-g',
      fromBeginning: true,
      allowAutoTopicCreation: true,
      retry: { initialRetryTime: 30_000, maxRetryTime: 30_000 },
    };

    @Injectable()
    class FirstMember {
      @Consumer('e2e-rebalance', options)
      async handle(msg: any) {
        handle(msg);
      }
    }

    // Same group, another topic: joining forces a rebalance that revokes and
    // re-assigns e2e-rebalance to the first member (its only subscriber).
    @Injectable()
    class SecondMember {
      @Consumer('e2e-rebalance-other', options)
      async handle() {}
    }

    const app1 = await createApp([FirstMember]);
    let app2: Awaited<ReturnType<typeof createApp>> | undefined;
    try {
      await app1.get(KafkaClient).send('e2e-rebalance', { value: { id: 'A' } });
      await waitFor(() => failures === 1);
      const t1 = Date.now();

      // The revoke resumes the paused partition and drops its retry state, so
      // after the re-assign A is redelivered without the 30s backoff.
      app2 = await createApp([SecondMember]);
      await waitFor(() => successes === 1, 25_000);
      await new Promise((r) => setTimeout(r, 3000));
      expect(successes).toBe(1);
      expect(Date.now() - t1).toBeLessThan(30_000);
    } finally {
      await app2?.close();
      await app1.close();
    }
  }, 90_000);

  it('boots and consumes with a consumer also exposed through useExisting', async () => {
    const received: string[] = [];

    @Injectable()
    class AliasedConsumer {
      @Consumer('e2e-alias', {
        groupId: 'e2e-alias-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
      })
      async handle(msg: any) {
        received.push(msg.value?.id);
      }
    }

    const app = await createApp([
      AliasedConsumer,
      { provide: 'ALIASED_CONSUMER', useExisting: AliasedConsumer },
    ]);
    try {
      await app.get(KafkaClient).send('e2e-alias', { value: { id: 'x' } });
      await waitFor(() => received.length >= 1);
      await new Promise((r) => setTimeout(r, 1500));
      expect(received).toEqual(['x']);
    } finally {
      await app.close();
    }
  });

  it('autoStart false starts on startAll', async () => {
    const received: string[] = [];

    @Injectable()
    class LateConsumer {
      @Consumer('e2e-autostart', {
        groupId: 'e2e-autostart-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
      })
      async handle(msg: any) {
        received.push(msg.value?.id);
      }
    }

    const app = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ clientId: 'e2e', brokers: [bootstrap] }),
        ConsumerModule.forRoot({ autoStart: false }),
      ],
      providers: [LateConsumer],
    }).compile();
    await app.init();
    try {
      await app.get(KafkaClient).send('e2e-autostart', { value: { id: 'x' } });
      await new Promise((r) => setTimeout(r, 2000));
      expect(received).toEqual([]);

      await app.get(ConsumerRegistryService).startAll();
      await waitFor(() => received.length >= 1);
      expect(received).toEqual(['x']);
    } finally {
      await app.close();
    }
  });

  it('dead-letters a non-UTF-8 payload byte-for-byte', async () => {
    const payload = Buffer.from([0xff, 0x00, 0xfe, 0x80]);
    const key = Buffer.from([0x80, 0x81]);
    const dlqReceived: any[] = [];

    @Injectable()
    class BinarySource {
      @Consumer('e2e-bin-src', {
        groupId: 'e2e-bin-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        deserialize: false,
        dlq: { topic: 'e2e-bin-dead', maxRetries: 0 },
      })
      async handle() {
        throw new Error('cannot decode');
      }
    }

    @Injectable()
    class BinaryDlqSpy {
      @Consumer('e2e-bin-dead', {
        groupId: 'e2e-bin-spy',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        deserialize: false,
      })
      async handle(msg: any) {
        dlqReceived.push(msg);
      }
    }

    const app = await createApp([BinarySource, BinaryDlqSpy]);
    try {
      await app.get(KafkaClient).send('e2e-bin-src', { key, value: payload });
      await waitFor(() => dlqReceived.length >= 1);
      expect(Buffer.from(dlqReceived[0].value).equals(payload)).toBe(true);
      expect(Buffer.from(dlqReceived[0].key).equals(key)).toBe(true);
    } finally {
      await app.close();
    }
  });

  it('delivers more than 32 messages to one batch call', async () => {
    const producerApp = await createApp([]);
    try {
      await producerApp.get(KafkaClient).sendBatch(
        'e2e-big-batch',
        Array.from({ length: 80 }, (_, i) => ({ value: { i } })),
      );
    } finally {
      await producerApp.close();
    }

    const sizes: number[] = [];

    @Injectable()
    class BigBatchConsumer {
      @Consumer('e2e-big-batch', {
        groupId: 'e2e-big-batch-g',
        fromBeginning: true,
        batch: true,
        batchSize: 100,
      })
      async handle(msgs: any[]) {
        sizes.push(msgs.length);
      }
    }

    const app = await createApp([BigBatchConsumer]);
    try {
      await waitFor(() => sizes.reduce((a, b) => a + b, 0) >= 80);
      expect(Math.max(...sizes)).toBeGreaterThan(32);
    } finally {
      await app.close();
    }
  });

  it('reports healthy with consumer details after bootstrap', async () => {
    @Injectable()
    class HealthConsumer {
      @Consumer('e2e-health', {
        groupId: 'e2e-health-g',
        allowAutoTopicCreation: true,
      })
      async handle() {}
    }

    const app = await createApp([HealthConsumer]);
    try {
      const health = app.get(KafkaHealthIndicator);
      await waitFor(
        () => health.isHealthy('kafka').kafka.status === 'up',
        10_000,
      );
      const { kafka: result } = health.isHealthy('kafka');
      expect(result.consumers).toEqual({ 'default:e2e-health-g': 'running' });
      expect(result.producers).toEqual({ default: true });

      // Two broker probes share one connected admin
      for (let i = 0; i < 2; i++) {
        const { b } = await health.checkBrokers('b');
        expect(b.status).toBe('up');
      }
    } finally {
      await app.close();
    }
  });
});

describe('DLQ on a separate cluster (dlq.connection)', () => {
  let dlqKafka: Awaited<ReturnType<GenericContainer['start']>>;
  const dlqBootstrap = '127.0.0.1:9094';

  beforeAll(async () => {
    // Broker auto-create OFF: the DLQ topics can only exist on this cluster if
    // the library's allowAutoTopicCreation created them on dlq.connection.
    dlqKafka = await startKafka(9094, false);
  }, 180_000);

  afterAll(async () => {
    await dlqKafka?.stop();
  });

  async function withAdmin<T>(
    brokers: string,
    fn: (admin: KafkaJS.Admin) => Promise<T>,
  ): Promise<T> {
    const admin = new KafkaJS.Kafka({
      kafkaJS: { brokers: [brokers] },
    }).admin();
    await admin.connect();
    try {
      return await fn(admin);
    } finally {
      await admin.disconnect();
    }
  }

  const messageCount = (brokers: string, topic: string) =>
    withAdmin(brokers, async (admin) => {
      if (!(await admin.listTopics()).includes(topic)) return 0;
      const offsets = await admin.fetchTopicOffsets(topic);
      return offsets.reduce((n, p) => n + Number(p.high), 0);
    });

  it('dead-letters on the DLQ cluster only', async () => {
    let sourceAttempts = 0;
    const successes: string[] = [];

    @Injectable()
    class CrossClusterConsumer {
      @Consumer('e2e-xdlq-src', {
        groupId: 'e2e-xdlq-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
        dlq: {
          topic: 'e2e-xdlq-dead',
          connection: 'dlq',
          maxRetries: 1,
          retryDelay: 100,
        },
      })
      async handle(msg: any) {
        if (msg.value?.fail) {
          sourceAttempts++;
          throw new Error('permanent');
        }
        successes.push(msg.value?.id);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ clientId: 'e2e', brokers: [bootstrap] }),
        KafkaModule.forRoot({
          name: 'dlq',
          clientId: 'e2e-dlq',
          brokers: [dlqBootstrap],
        }),
        ConsumerModule.forRoot(),
      ],
      providers: [CrossClusterConsumer],
    }).compile();
    await moduleRef.init();

    try {
      const client = moduleRef.get(KafkaClient);
      await client.send('e2e-xdlq-src', { value: { id: 'x', fail: true } });
      await client.send('e2e-xdlq-src', { value: { id: 'good' } });

      // 'good' comes after the poison message on the same partition, so its
      // arrival means the poison message has been dead-lettered.
      await waitFor(() => successes.includes('good'));

      expect(sourceAttempts).toBe(2); // 1 initial + 1 retry before DLQ
      expect(await messageCount(dlqBootstrap, 'e2e-xdlq-dead')).toBe(1);

      const mainTopics = await withAdmin(bootstrap, (a) => a.listTopics());
      expect(mainTopics).toContain('e2e-xdlq-src');
      expect(mainTopics).not.toContain('e2e-xdlq-dead');
    } finally {
      await moduleRef.close();
    }
  });

  it('two forRoot() calls: a consumer on the second connection receives', async () => {
    const received: any[] = [];

    @Injectable()
    class SecondConnectionConsumer {
      @Consumer('e2e-two-roots', {
        connection: 'second',
        groupId: 'e2e-two-roots-g',
        fromBeginning: true,
        allowAutoTopicCreation: true,
      })
      async handle(msg: any) {
        received.push(msg);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ clientId: 'e2e', brokers: [bootstrap] }),
        KafkaModule.forRoot({
          name: 'second',
          clientId: 'e2e-2',
          brokers: [dlqBootstrap],
        }),
        ConsumerModule.forRoot(),
      ],
      providers: [SecondConnectionConsumer],
    }).compile();
    await moduleRef.init();

    try {
      await moduleRef
        .get(KafkaClient)
        .send(
          'e2e-two-roots',
          { value: { id: 'r' } },
          { connection: 'second' },
        );
      await waitFor(() => received.length >= 1);
      expect(received[0].value).toEqual({ id: 'r' });

      const secondTopics = await withAdmin(dlqBootstrap, (a) => a.listTopics());
      const mainTopics = await withAdmin(bootstrap, (a) => a.listTopics());
      expect(secondTopics).toContain('e2e-two-roots');
      expect(mainTopics).not.toContain('e2e-two-roots');
    } finally {
      await moduleRef.close();
    }
  });

  it('fails at startup when dlq.connection is not registered', async () => {
    @Injectable()
    class BadDlqConsumer {
      @Consumer('e2e-xdlq-bad', {
        groupId: 'e2e-xdlq-bad-g',
        dlq: { topic: 'e2e-xdlq-bad-dead', connection: 'missing' },
      })
      async handle() {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ clientId: 'e2e', brokers: [bootstrap] }),
        ConsumerModule.forRoot(),
      ],
      providers: [BadDlqConsumer],
    }).compile();
    try {
      await expect(moduleRef.init()).rejects.toThrow(
        'Kafka connection "missing" not found',
      );
    } finally {
      await moduleRef.close().catch(() => undefined);
    }
  });
});
