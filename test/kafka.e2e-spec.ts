import { GenericContainer, Wait } from 'testcontainers';
import { Test } from '@nestjs/testing';
import { Injectable } from '@nestjs/common';
import { KafkaModule, ConsumerModule, Consumer, KafkaClient } from '../lib';

let kafka: Awaited<ReturnType<GenericContainer['start']>>;
let bootstrap: string;

beforeAll(async () => {
  // Self-contained single-node KRaft Kafka with a FIXED host port and the
  // advertised listener pointing at 127.0.0.1:9092 — no mapped/advertised
  // port mismatch possible. 127.0.0.1 (not "localhost") because librdkafka
  // resolves localhost to IPv6 ::1, where Docker's published ports don't listen.
  kafka = await new GenericContainer('confluentinc/cp-kafka:7.6.0')
    .withEnvironment({
      CLUSTER_ID: '5L6g3nShT-eMCtK--X86sw',
      KAFKA_NODE_ID: '1',
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093',
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093',
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://127.0.0.1:9092',
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
        'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true',
    })
    .withExposedPorts({ container: 9092, host: 9092 })
    .withWaitStrategy(Wait.forLogMessage('Kafka Server started'))
    .start();
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

async function createApp(providers: any[]) {
  const moduleRef = await Test.createTestingModule({
    imports: [
      KafkaModule.forRoot({ clientId: 'e2e', brokers: [bootstrap] }),
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

  it('retries a failing handler in-process, then keeps consuming (regression: fix #4)', async () => {
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
        batchTimeout: 300,
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

  it('app.close() completes promptly with a long retry delay in flight (regression: fix #3)', async () => {
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
        // skip=false → infinite in-process retry; shutdown aborts with a
        // one-shot rethrow so the offset is never committed.
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

      await waitFor(() => attempts >= 1); // poison is inside the retry sleep
      expect(app1Successes).toEqual([]); // 'good' not reached yet (partition order)
    } finally {
      await app1.close(); // aborts the 60s retry sleep
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

      // 3 attempts BEYOND maxRetries=2 — all in-process, no consumer restart, app stays up
      await waitFor(
        () => successes.includes('poison') && successes.includes('good'),
        60_000,
      );
      expect(attempts).toBe(5);
    } finally {
      await app.close();
    }
  });
});
