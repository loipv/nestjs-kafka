/**
 * What `commitMode: 'sync'` (one synchronous commit per message) costs against
 * a real broker. Manual only: needs Docker and the native librdkafka binding.
 * Run: `npm run bench:kafka`. With Colima, point testcontainers at its socket:
 * `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
 *  TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock npm run bench:kafka`
 */
import { GenericContainer, Wait } from 'testcontainers';
import { Test } from '@nestjs/testing';
import { Injectable, Logger } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import {
  KafkaModule,
  ConsumerModule,
  Consumer,
  KafkaClient,
  ConsumerOptions,
} from '../../lib';

const PORT = 9094; // never collides with the e2e suite (9092)
const BOOTSTRAP = `127.0.0.1:${PORT}`;
const N = 20_000;
const PRODUCE_CHUNK = 1000;

let kafka: Awaited<ReturnType<GenericContainer['start']>>;

// Same single-node KRaft broker as test/kafka.e2e-spec.ts
function startKafka() {
  return new GenericContainer('confluentinc/cp-kafka:7.6.0')
    .withEnvironment({
      CLUSTER_ID: '5L6g3nShT-eMCtK--X86sw',
      KAFKA_NODE_ID: '1',
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093',
      KAFKA_LISTENERS: `PLAINTEXT://0.0.0.0:${PORT},CONTROLLER://0.0.0.0:9093`,
      KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://${BOOTSTRAP}`,
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
        'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false',
    })
    .withExposedPorts({ container: PORT, host: PORT })
    .withWaitStrategy(Wait.forLogMessage('Kafka Server started'))
    .start();
}

async function createTopics() {
  const admin = new KafkaJS.Kafka({
    kafkaJS: { brokers: [BOOTSTRAP] },
  }).admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [
        { topic: 'bench-1p', numPartitions: 1, replicationFactor: 1 },
        { topic: 'bench-4p', numPartitions: 4, replicationFactor: 1 },
      ],
    });
  } finally {
    await admin.disconnect();
  }
}

async function produce(topics: string[]) {
  const moduleRef = await Test.createTestingModule({
    imports: [
      KafkaModule.forRoot({ clientId: 'bench-producer', brokers: [BOOTSTRAP] }),
    ],
  }).compile();
  await moduleRef.init();
  const client = moduleRef.get(KafkaClient);
  for (const topic of topics) {
    for (let i = 0; i < N; i += PRODUCE_CHUNK) {
      await client.sendBatch(
        topic,
        Array.from({ length: PRODUCE_CHUNK }, (_, j) => ({
          key: `k${(i + j) % 64}`,
          value: { id: i + j },
        })),
      );
    }
  }
  await moduleRef.close();
}

/** Consumes N messages; returns msg/s from the first message to the N-th. */
async function consume(
  topic: string,
  groupId: string,
  options: Partial<ConsumerOptions>,
): Promise<number> {
  let count = 0;
  let first = 0n;
  let last = 0n;
  let done!: () => void;
  const finished = new Promise<void>((resolve) => (done = resolve));

  @Injectable()
  class BenchConsumer {
    @Consumer(topic, { groupId, fromBeginning: true, ...options })
    handle() {
      const now = process.hrtime.bigint();
      if (count === 0) first = now;
      count++;
      if (count === N) {
        last = now;
        done();
      }
      return Promise.resolve();
    }
  }

  const moduleRef = await Test.createTestingModule({
    imports: [
      KafkaModule.forRoot({ clientId: 'bench', brokers: [BOOTSTRAP] }),
      ConsumerModule.forRoot(),
    ],
    providers: [BenchConsumer],
  }).compile();
  await moduleRef.init();
  try {
    await finished;
  } finally {
    await moduleRef.close();
  }
  expect(count).toBe(N);
  // N - 1 intervals between the first and the N-th message
  return Math.round(((N - 1) / Number(last - first)) * 1e9);
}

const scenarios: Array<{
  name: string;
  topic: string;
  options: Partial<ConsumerOptions>;
}> = [
  {
    name: 'commitMode:background',
    topic: 'bench-1p',
    options: { commitMode: 'background' },
  },
  {
    name: 'commitMode:sync',
    topic: 'bench-1p',
    options: { commitMode: 'sync' },
  },
  {
    name: 'commitMode:sync x4',
    topic: 'bench-4p',
    options: { commitMode: 'sync', partitionsConsumedConcurrently: 4 },
  },
];

describe('commitMode cost on Kafka', () => {
  beforeAll(async () => {
    Logger.overrideLogger(['error']);
    kafka = await startKafka();
    await createTopics();
    await produce(['bench-1p', 'bench-4p']);
  }, 300_000);

  afterAll(async () => {
    await kafka?.stop();
  });

  scenarios.forEach(({ name, topic, options }, i) => {
    it(name, async () => {
      const perSecond = await consume(topic, `bench-group-${i}`, options);
      process.stdout.write(
        `BENCH ${name.padEnd(20)} ${String(perSecond).padStart(8)} msg/s (N=${N}, ${topic})\n`,
      );
    });
  });
});
