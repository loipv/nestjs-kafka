# @loipv/nestjs-kafka

A production-ready NestJS module for Kafka client and consumer functionality built on top of [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript). It provides batch processing, non-blocking retry (pause the partition, redeliver) with a Dead Letter Queue, idempotency, key-based grouping, multi-connection support and OpenTelemetry tracing.

> **Note:** Starting from v1.0.0, this library uses `@confluentinc/kafka-javascript` instead of `kafkajs` for better performance and official Confluent support. See [Migration Guide](#migration-guide-from-v0x-to-v1x) for upgrade instructions.

## Features

- **Producer (KafkaClient)**: `send()`, `sendBatch()`, `sendMultiTopicBatch()`, on any registered connection
- **Consumer**: Method decorator-based consumer with auto-discovery
- **Batch Processing**: Batch handlers with a configurable maximum batch size
- **Key-Based Grouping**: Group messages by key within batches for ordered processing
- **Idempotency**: In-memory duplicate prevention with TTL
- **Retry and Dead Letter Queue (DLQ)**: Failed messages are redelivered with exponential backoff while only their partition is paused, then an optional DLQ topic
- **OpenTelemetry Tracing**: Distributed tracing across produce → consume with same trace ID
- **Health Checks**: Producer and consumer-group readiness for `@nestjs/terminus`
- **TLS**: Certificates through `ssl`, any librdkafka property through `rdkafka`
- **Multi-connection**: Several Kafka clusters in one app
- **Graceful Shutdown**: Proper cleanup on application shutdown

## Installation

```bash
npm install @loipv/nestjs-kafka @confluentinc/kafka-javascript
```

### Peer Dependencies

Make sure you have the following peer dependencies installed:

```bash
npm install @nestjs/common @nestjs/core @nestjs/terminus reflect-metadata rxjs
```

### Platform Support

confluent-kafka-javascript is built on librdkafka (C library). Supported platforms:
- **Linux**: x64, arm64
- **macOS**: arm64 (Apple Silicon)
- **Windows**: x64
- **Node.js**: 20, 22, 24 (`engines: >=20`)

### Optional: OpenTelemetry Tracing

For distributed tracing support:

```bash
npm install @opentelemetry/api @opentelemetry/sdk-trace-node @opentelemetry/auto-instrumentations-node
```

## Quick Start

### 1. Import KafkaModule

```typescript
// app.module.ts (Root Module)
import { Module } from '@nestjs/common';
import { KafkaModule, ConsumerModule } from '@loipv/nestjs-kafka';
import { OrderModule } from './order/order.module';

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    }),
    // ConsumerModule with default options (applied to all @Consumer decorators)
    ConsumerModule.forRoot({
      partitionAssigners: ['cooperative-sticky'],  // Default for all consumers
      allowAutoTopicCreation: true,                // Auto-create topics
    }),
    OrderModule,
  ],
})
export class AppModule {}
```

```typescript
// order/order.module.ts (Feature Module)
import { Module } from '@nestjs/common';
import { OrderConsumer } from './order.consumer';
import { OrderService } from './order.service';

@Module({
  // No need to import ConsumerModule — consumers are auto-discovered!
  // Just declare your consumer and its dependencies as providers.
  providers: [OrderConsumer, OrderService],
})
export class OrderModule {}
```

### 2. Create a Consumer

```typescript
import { Injectable } from '@nestjs/common';
import { Consumer, KafkaMessagePayload } from '@loipv/nestjs-kafka';

@Injectable()
export class OrderConsumer {
  @Consumer('orders')
  async handleOrder(message: KafkaMessagePayload<Order>) {
    // Values are deserialized by default: JSON becomes an object, anything else a string
    console.log('Processing order:', message.value.orderId);
  }
}
```

### 3. Use the Producer

```typescript
import { Injectable } from '@nestjs/common';
import { KafkaClient } from '@loipv/nestjs-kafka';

@Injectable()
export class OrderService {
  constructor(private readonly kafka: KafkaClient) {}

  async createOrder(order: Order) {
    await this.kafka.send('orders', {
      key: order.customerId,
      value: order, // Automatically serialized to JSON
    });
  }
}
```

## Configuration

### KafkaModule Options

```typescript
KafkaModule.forRoot({
  // Required
  clientId: 'my-app',
  brokers: ['localhost:9092'],

  // Optional - TLS/SASL (see "TLS and librdkafka properties" below)
  ssl: true,
  sasl: {
    mechanism: 'scram-sha-256',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },

  // Optional - Connection settings
  connectionTimeout: 3000,
  requestTimeout: 30000,

  // Optional - Retry configuration
  retry: {
    initialRetryTime: 100,
    retries: 8,
    maxRetryTime: 30000,
  },

  // Optional - Logging
  logLevel: 'INFO', // 'NOTHING' | 'ERROR' | 'WARN' | 'INFO' | 'DEBUG'
});
```

### TLS and librdkafka properties

`ssl: true` enables TLS with the system CA store. Pass an object to supply certificates; each option becomes a librdkafka property:

```typescript
KafkaModule.forRoot({
  clientId: 'my-app',
  brokers: ['broker:9093'],
  ssl: {
    ca: fs.readFileSync('ca.pem'),           // or an array of PEMs
    cert: fs.readFileSync('client-cert.pem'),
    key: fs.readFileSync('client-key.pem'),
    passphrase: process.env.KEY_PASSWORD,
    rejectUnauthorized: true,                 // false disables certificate verification
  },
});
```

| `ssl` option | librdkafka property |
|--------------|---------------------|
| `ca` | `ssl.ca.pem` (several PEMs are joined) |
| `cert` | `ssl.certificate.pem` |
| `key` | `ssl.key.pem` |
| `passphrase` | `ssl.key.password` |
| `rejectUnauthorized: false` | `enable.ssl.certificate.verification: false` |

Any other key in the `ssl` object fails at startup. For anything else, pass raw [librdkafka properties](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) through `rdkafka`. They apply to the connection's producer, consumers and admin clients, and override the properties built from `ssl` and the other connection options. Two consumer properties the library sets itself win over `rdkafka`: `js.consumer.max.batch.size` (from `batchSize`) and `rebalance_cb`. A consumer-only property also reaches the producer and admin clients, where librdkafka logs a warning and ignores it. `enable.auto.commit` is rejected at startup: set `autoCommit` on the consumer instead, so the library knows when to commit itself:

```typescript
KafkaModule.forRoot({
  clientId: 'my-app',
  brokers: ['broker:9093'],
  ssl: true,
  rdkafka: {
    'ssl.ca.location': '/etc/ssl/kafka-ca.pem',
    'ssl.endpoint.identification.algorithm': 'none',
    'socket.keepalive.enable': true,
  },
});
```

### Async Configuration

```typescript
KafkaModule.forRootAsync({
  imports: [ConfigModule],
  useFactory: (config: ConfigService) => ({
    clientId: config.get('KAFKA_CLIENT_ID'),
    brokers: config.get('KAFKA_BROKERS').split(','),
  }),
  inject: [ConfigService],
});
```

### ConsumerModule Options

`ConsumerModule.forRoot()` accepts optional default options that apply to all `@Consumer` decorators. All consumers are **automatically discovered** via their `@Consumer()` decorator — no explicit registration needed. Decorator options take precedence over module defaults.

```typescript
ConsumerModule.forRoot({
  // Partition assignment strategy (default for all consumers)
  partitionAssigners: ['cooperative-sticky'],

  // Auto-create topics if they don't exist
  allowAutoTopicCreation: true,

  // Consumer group settings
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  rebalanceTimeout: 60000,

  // Commit settings
  autoCommit: true,
  autoCommitInterval: 5000,
  fromBeginning: false,

  // Retry options
  retry: {
    retries: 3,
    initialRetryTime: 1000,
    multiplier: 2,
  },

  // Max idempotency keys kept in memory (oldest evicted first)
  idempotencyMaxKeys: 100000,
});
```

**How defaults work:**

```typescript
// Module defaults
ConsumerModule.forRoot({
  partitionAssigners: ['cooperative-sticky'],
  sessionTimeout: 30000,
});

// This consumer uses module defaults (cooperative-sticky, 30s timeout)
@Consumer('orders')
async handleOrder(message: KafkaMessagePayload) {}

// This consumer overrides partitionAssigners but keeps sessionTimeout from defaults
@Consumer('events', { partitionAssigners: ['roundrobin'] })
async handleEvent(message: KafkaMessagePayload) {}
```

**ConsumerModuleOptions:**

| Option | Type | Description |
|--------|------|-------------|
| `partitionAssigners` | `PartitionAssigner[]` | Default partition assignment strategies |
| `allowAutoTopicCreation` | `boolean` | Auto-create topics if not exist |
| `sessionTimeout` | `number` | Session timeout in ms |
| `heartbeatInterval` | `number` | Heartbeat interval in ms |
| `rebalanceTimeout` | `number` | Rebalance timeout in ms |
| `autoCommit` | `boolean` | Commit in the background (default `true`); `false` commits synchronously after each handled message or chunk |
| `autoCommitInterval` | `number` | Auto commit interval in ms |
| `fromBeginning` | `boolean` | Start consuming from beginning |
| `retry` | `ConsumerRetryOptions` | Default retry options |
| `idempotencyMaxKeys` | `number` | Max idempotency keys kept in memory, oldest evicted first (default: 100000) |
| `autoStart` | `boolean` | Start consumers at bootstrap (default: `true`); `false` waits for `startAll()` |

### Startup and shutdown order

Consumers start in `ConsumerModule`'s `onApplicationBootstrap`. Every provider's `onModuleInit` has run by then, but `ConsumerModule` is global and Nest bootstraps global modules **first**: other modules' `onApplicationBootstrap` may still be running when the first message arrives. Prepare what handlers need in `onModuleInit`, or start consumers yourself:

```typescript
ConsumerModule.forRoot({ autoStart: false });

// main.ts
const app = await NestFactory.create(AppModule);
await app.listen(3000);
await app.get(ConsumerRegistryService).startAll();
```

With `autoStart: false`, discovery and option checks still run at bootstrap (a bad option still fails startup). `startAll()` can be called more than once; if starting fails it closes what it started (consumers and producers) and rejects. It rejects when called before the application is initialised (call it after `app.init()` / `app.listen()`, not right after `NestFactory.create()`) and once shutdown has begun; a shutdown that arrives while consumers are starting stops them. A failed `startAll()` also closes the producers and cannot be retried (later calls return the same rejection), so exit the process on failure: `await registry.startAll().catch(() => process.exit(1))`.

`startAll()` is the only `ConsumerRegistryService` method meant for applications; the others are internal.

With the default `autoStart: true`, exit the process when bootstrap fails (`bootstrap().catch(() => process.exit(1))`): Nest skips shutdown hooks after a failed initialisation, so started consumers would keep the process alive.

On shutdown, consumers stop in `onModuleDestroy` and producers disconnect in `onApplicationShutdown`, so consumers always stop first. Nest destroys global modules **last**, so your own `onModuleDestroy` runs while consumers still run: release resources handlers use (database pools, HTTP clients) in `beforeApplicationShutdown` or `onApplicationShutdown`.

### Consumer discovery

Every `@Consumer` method on a singleton provider is discovered, including methods inherited from a base class (a subclass override without `@Consumer` is not a consumer). A provider also exposed under another token (`useExisting`) is registered once. Request-scoped providers, providers that depend on one, and transient providers cannot consume (Nest has no fully built instance for them at bootstrap): they are skipped with a warning naming the reason. If a base class with `@Consumer` methods is itself registered as a provider and so is a subclass, both handle the same topic and startup fails with "already has a handler"; register only one of them.

### Multi-Connection (Multiple Kafka Clusters)

Connect to multiple Kafka clusters simultaneously:

```typescript
// One forRoot() (or forRootAsync()) call per connection, each with its own name
@Module({
  imports: [
    // Primary cluster (default connection)
    KafkaModule.forRoot({
      name: 'default',  // Optional, 'default' is the default
      clientId: 'my-app',
      brokers: ['primary-kafka:9092'],
    }),
    // Secondary cluster
    KafkaModule.forRoot({
      name: 'analytics',
      clientId: 'my-app-analytics',
      brokers: ['analytics-kafka:9092'],
    }),
    ConsumerModule.forRoot(),
  ],
})
export class AppModule {}
```

Each connection name is registered once: a second `forRoot()` with the same name and different options fails at startup (`Kafka connection "<name>" is already registered with different options (<keys>)`). All connections share one `KafkaClient` and one consumer registry, so a consumer or a send can target any of them by name. Inject per-connection options with `@Inject(getKafkaOptionsToken('analytics'))` (`getKafkaOptionsToken()` for the default connection).

**Using named connections in Consumer:**

```typescript
@Injectable()
export class EventConsumer {
  // Default connection
  @Consumer('orders')
  async handleOrders(message: KafkaMessagePayload) {
    // Uses 'default' connection
  }

  // Specific connection
  @Consumer('analytics-events', { connection: 'analytics' })
  async handleAnalytics(message: KafkaMessagePayload) {
    // Uses 'analytics' connection
  }
}
```

**Using named connections in Producer:**

```typescript
@Injectable()
export class EventService {
  constructor(
    // Method 1: Use @InjectKafkaClient decorator
    @InjectKafkaClient() private readonly kafka: KafkaClient,
    @InjectKafkaClient('analytics') private readonly analyticsKafka: ConnectionBoundClient,
  ) {}

  async sendToDefault() {
    await this.kafka.send('orders', { value: data });
  }

  async sendToAnalytics() {
    await this.analyticsKafka.send('events', { value: data });
  }
}

// Method 2: Pass the connection per send
@Injectable()
export class AnotherService {
  constructor(private readonly kafka: KafkaClient) {}

  async sendToAnalytics() {
    await this.kafka.send('events', { value: data }, { connection: 'analytics' });
  }
}
```

## Consumer Options

### Basic Consumer

```typescript
@Consumer('topic-name')
async handleMessage(message: KafkaMessagePayload) {
  // Process single message
}
```

### Batch Consumer

```typescript
@Consumer('orders', {
  batch: true,
  batchSize: 100, // Max messages per handler call (default: 100)
})
async handleBatch(messages: KafkaMessagePayload[]) {
  // Process batch of messages
}
```

The handler receives the messages the client fetched for one partition, split into chunks of at most `batchSize`. The client fetches at most 32 messages per partition at a time, so a handler call never holds more than 32 messages.

All consumers sharing a `groupId` must be either batch or non-batch; mixing them fails at startup.

**Delivery semantics (at-least-once):** offsets are committed only after a batch is processed successfully. If the process crashes mid-batch (or the handler keeps failing with `skipMessageOnMaxRetries: false`), the whole batch is redelivered — make your batch handler idempotent (see [Consumer with Idempotency](#consumer-with-idempotency)). Batch handlers get the same retry/DLQ wiring as single-message handlers.

### Batch with Key Grouping

```typescript
@Consumer('orders', {
  batch: true,
  batchSize: 100,
  groupByKey: true,  // Group messages by key within batch
})
async handleBatch(groupedMessages: GroupedBatch[]) {
  // groupedMessages = [{ key: 'customer-1', messages: [...] }, ...]
  for (const group of groupedMessages) {
    console.log(`Processing ${group.messages.length} orders for ${group.key}`);
  }
}
```

### Consumer with DLQ

```typescript
@Consumer('payments', {
  dlq: {
    topic: 'payments-dlq',
    maxRetries: 3,           // Retry 3 times before DLQ
    retryDelay: 1000,        // Initial delay: 1 second
    retryBackoffMultiplier: 2, // Exponential backoff
    includeErrorInfo: true,  // Include error in DLQ headers
    sendTimeout: 30000,      // A slower DLQ send counts as failed
  },
})
async handlePayment(message: KafkaMessagePayload) {
  // If this throws, message will be retried then sent to DLQ
}
```

Retries work as described in [Retry Mechanism](#retry-mechanism): the partition is paused between attempts. The DLQ backoff is capped by `retry.maxRetryTime` (default 30s).

The DLQ message carries the original value and key unchanged, so binary payloads (Avro, Protobuf) arrive byte-for-byte. A batch consumer dead-letters the failing chunk (one send per message, batched by librdkafka), without messages the idempotency check filtered out.

If sending to the DLQ fails or exceeds `sendTimeout`, the message is not acknowledged: it is retried with the same backoff. Nothing is dropped. A retry only sends the messages of the chunk that did not reach the DLQ yet. The DLQ can still hold a duplicate (at-least-once): a send that timed out may land later, and a process that stops between the DLQ send and the offset commit dead-letters the message again after the restart (`autoCommit: false` narrows that window).

**DLQ Headers:**

| Header | Description |
|--------|-------------|
| `x-dlq-original-topic` | Original topic name |
| `x-dlq-handler-retry-count` | Attempts before the message was dead-lettered |
| `x-dlq-timestamp` | Timestamp when sent to DLQ |
| `x-dlq-error-message` | Error message (first 1000 characters) |
| `x-dlq-error-stack` | Error stack (first 1000 characters) |

To reprocess dead-lettered messages, add an ordinary `@Consumer` on the DLQ topic.

### DLQ on a Separate Cluster

By default the DLQ topic lives on the consumer's own connection. Set `dlq.connection` to any registered connection to produce the DLQ there instead:

```typescript
@Consumer('payments', {
  connection: 'main',
  dlq: {
    topic: 'payments-dlq',
    connection: 'dlq-cluster', // the DLQ is produced on this connection
  },
})
```

An unknown `dlq.connection` fails at startup. With `allowAutoTopicCreation`, the DLQ topic is auto-created on the DLQ connection.

### Consumer with Idempotency

```typescript
@Consumer('events', {
  idempotencyKey: (msg) => msg.headers?.['event-id']?.toString(),
  idempotencyTtl: 3600000, // 1 hour
})
async handleEvent(message: KafkaMessagePayload) {
  // Duplicate messages (same event-id) will be skipped
}
```

Keys are scoped to the consumer (connection, group and topic), so two consumers extracting the same key never skip each other's messages. The store is in memory: it holds at most `idempotencyMaxKeys` keys (default 100000, oldest evicted first) and does not survive a restart. A key is recorded only after the handler succeeds.

### Disabled Consumer

```typescript
@Consumer('orders', {
  disabled: true, // Consumer will be skipped during registration
})
async handleOrder(message: KafkaMessagePayload) {
  // This handler will not be registered
}
```

Use this to temporarily disable a consumer without removing the code.

### Partition Assignment Strategy

Control how partitions are assigned to consumers in a consumer group:

```typescript
@Consumer('orders', {
  groupId: 'order-processors',
  // Use cooperative-sticky for minimal rebalancing disruption
  partitionAssigners: ['cooperative-sticky'],
})
async handleOrder(message: KafkaMessagePayload) {
  // Process order
}

// Multiple strategies (first one is primary)
@Consumer('events', {
  partitionAssigners: ['roundrobin', 'range'],
})
async handleEvent(message: KafkaMessagePayload) {
  // Process event
}
```

**Available strategies:**
- `'roundrobin'` - Assigns partitions in round-robin fashion across consumers
- `'range'` - Assigns partitions based on ranges (default)
- `'cooperative-sticky'` - Cooperative rebalancing with sticky assignment (recommended for minimal disruption during rebalancing)

### Auto-Deserialization

Messages are automatically deserialized by default:

- **JSON**: Parsed automatically if valid JSON
- **String**: Falls back to UTF-8 string
- **Key**: Buffer converted to string

```typescript
// With auto-deserialization (default)
@Consumer('orders')
async handleOrder(message: KafkaMessagePayload<Order>) {
  // message.value is already parsed as Order object
  // message.key is string (not Buffer)
  console.log(message.value.orderId);
}

// Disable auto-deserialization for raw Buffer access
@Consumer('binary-data', { deserialize: false })
async handleBinary(message: KafkaJS.KafkaMessage) { // import { KafkaJS } from '@confluentinc/kafka-javascript'
  // message.value is Buffer
  const raw = message.value.toString('hex');
}
```

### Retry Mechanism

A failing handler is never re-run inside the same call. The library **pauses the message's partition, rethrows, and resumes the partition after the backoff delay**; the client then redelivers the message. The consumer keeps polling, so other partitions keep flowing and a long retry never gets the member evicted from its group.

```typescript
@Consumer('orders', {
  retry: {
    retries: 3,                    // skip/DLQ milestone (see below)
    initialRetryTime: 1000,        // Default: 1000ms
    multiplier: 2,                 // Default: 2 (exponential backoff)
    maxRetryTime: 30000,           // Default: 30000ms — cap for every delay
    skipMessageOnMaxRetries: false, // Default: false (retry indefinitely)
  },
})
async handleOrders(message: KafkaMessagePayload) {
  // If this fails, it is redelivered after 1s, 2s, 4s ... capped at 30s
}

// Skip a message that keeps failing
@Consumer('non-critical-logs', {
  retry: {
    retries: 5,
    skipMessageOnMaxRetries: true, // Skip message after max retries
  },
})
async handleLogs(message: KafkaMessagePayload) {
  // If this fails 5 times, message is skipped (offset committed)
}
```

**Policy:**
- **Handler errors never crash the consumer.** With `skipMessageOnMaxRetries: false` (default) the message is retried **indefinitely** and never lost. `retries` only marks when the skip/DLQ decision happens.
- **Skip mode** (`skipMessageOnMaxRetries: true`): the message is skipped after `retries` attempts and its offset is committed.
- **With DLQ**: the message is dead-lettered after `dlq.maxRetries` attempts.
- **Batch consumers** follow the same policy per chunk of `batchSize`. Each successful chunk is committed; a failing chunk pauses the partition and is redelivered, earlier chunks are not reprocessed. Batch handlers must still be idempotent (at-least-once).
- **Infra failures** (lost broker connection) never stop the consumer: librdkafka reconnects on its own and consumption resumes. An error while starting a consumer (connect, subscribe) fails application startup.
- **Shutdown** while a partition is paused never commits the failing message: it is redelivered on the next start.
- **Timer limits**: `retry.initialRetryTime`, `retry.maxRetryTime`, `dlq.sendTimeout` and `dlq.retryDelay` must be positive and at most 2147483647 ms (Node's `setTimeout` limit); `retry.multiplier` and `dlq.retryBackoffMultiplier` must be finite and >= 1. Other values fail at startup.
- **`autoCommit: false`** commits each handled message (or batch chunk) synchronously, including skipped and dead-lettered ones. A failed commit is logged, not retried: that partition's next successful commit covers it; if none follows (a quiet partition), the message is redelivered after a restart (at-least-once).

Attempt counters live in memory: a restart or a rebalance that moves the partition restarts the backoff ladder (the message itself is never lost). Each failed attempt is logged twice: once by this library, once by the Kafka client ("The same message may be reprocessed").

### All Consumer Options

```typescript
interface ConsumerOptions {
  // Multi-connection
  connection?: string;           // Default: 'default'

  // Enable/disable consumer
  disabled?: boolean;            // Default: false (skip registration when true)

  // Message deserialization
  deserialize?: boolean;         // Default: true (auto JSON parse/string)

  // Consumer group settings
  groupId?: string;              // Default: `${topic}-group`
  sessionTimeout?: number;
  heartbeatInterval?: number;
  rebalanceTimeout?: number;
  partitionsConsumedConcurrently?: number;

  // Batch processing (every consumer in a group must agree on `batch`)
  batch?: boolean;
  batchSize?: number;            // Default: 100 (also sets how many messages the client fetches per call)
  groupByKey?: boolean;

  // Auto-created topic sizing (when allowAutoTopicCreation: true)
  autoCreateTopicPartitions?: number;        // Default: 1
  autoCreateTopicReplicationFactor?: number; // Default: 1

  // Idempotency
  idempotencyKey?: (message: KafkaJS.KafkaMessage) => string | undefined;
  idempotencyTtl?: number;        // Default: 3600000 (1 hour)

  // Dead Letter Queue
  dlq?: {
    topic: string;
    connection?: string;          // Default: the consumer's connection
    maxRetries?: number;          // Default: 3
    retryDelay?: number;          // Default: 1000
    retryBackoffMultiplier?: number; // Default: 2
    includeOriginalHeaders?: boolean; // Default: true
    includeErrorInfo?: boolean;   // Default: true
    sendTimeout?: number;         // Default: 30000 (at most 2147483647)
  };

  // Commit settings
  autoCommit?: boolean;           // Default: true. false: commit synchronously after each
                                  // handled message / batch chunk (fewer redeliveries, lower throughput)
  autoCommitInterval?: number;
  fromBeginning?: boolean;        // Default: false

  // Topic creation
  allowAutoTopicCreation?: boolean; // Default: false

  // Partition assignment strategy
  partitionAssigners?: PartitionAssigner[];  // 'roundrobin' | 'range' | 'cooperative-sticky'

  // Retry of a failing handler (pause partition, redeliver)
  retry?: {
    retries?: number;             // Default: 3 (skip/DLQ milestone, see above)
    initialRetryTime?: number;    // Default: 1000 (positive, at most 2147483647)
    multiplier?: number;          // Default: 2
    maxRetryTime?: number;        // Default: 30000 (cap for every delay, DLQ included; at most 2147483647)
    skipMessageOnMaxRetries?: boolean; // Default: false (retry indefinitely)
  };
}
```

## Producer API

### KafkaClient Methods

```typescript
// Send single message
await kafka.send('topic', {
  key: 'message-key',
  value: { data: 'value' }, // Auto-serialized
  headers: { 'correlation-id': '123' },
});

// Send batch to single topic
await kafka.sendBatch('topic', [
  { key: 'key1', value: 'value1' },
  { key: 'key2', value: 'value2' },
]);

// Send to multiple topics
await kafka.sendMultiTopicBatch([
  { topic: 'topic1', messages: [{ value: 'msg1' }] },
  { topic: 'topic2', messages: [{ value: 'msg2' }] },
]);
```

### Producer Options

> **Note:** `acks`, `timeout`, `compression` and the other producer settings are configured per connection in `KafkaModule.forRoot()`, not per send. librdkafka batches sends internally, so there is no separate queued-send API.

```typescript
// Configure producer options at module level
KafkaModule.forRoot({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  producer: {
    acks: -1,          // -1 (all), 0 (none), 1 (leader only)
    timeout: 30000,
    compression: 'gzip', // 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd', or 0-4
    idempotent: true,
  },
});

// Send messages (acks/timeout/compression configured above)
await kafka.send('topic', {
  key: 'message-key',
  value: { data: 'value' },
});
```

## Health Checks

Integrate with `@nestjs/terminus`:

> **Note:** `KafkaHealthIndicator` returns plain `HealthIndicatorResult` objects. Terminus' `HealthCheckService` (import `TerminusModule`) turns a `down` result into a failed check; without Terminus you can read the result directly.

```typescript
// app.module.ts
import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { KafkaModule, ConsumerModule } from '@loipv/nestjs-kafka';

@Module({
  imports: [
    TerminusModule,  // Required for health checks
    KafkaModule.forRoot({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    }),
    ConsumerModule.forRoot(),
  ],
})
export class AppModule {}
```

```typescript
// health.controller.ts
import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { KafkaHealthIndicator } from '@loipv/nestjs-kafka';

@Controller('health')
export class HealthController {
  constructor(
    private health: HealthCheckService,
    private kafkaHealth: KafkaHealthIndicator,
  ) {}

  @Get()
  @HealthCheck()
  check() {
    return this.health.check([
      () => this.kafkaHealth.isHealthy('kafka'),
    ]);
  }

  @Get('kafka/brokers')
  @HealthCheck()
  checkBrokers() {
    return this.health.check([
      () => this.kafkaHealth.checkBrokers('kafka-brokers'),
    ]);
  }

  @Get('kafka/lag')
  @HealthCheck()
  checkLag() {
    return this.health.check([
      () => this.kafkaHealth.checkConsumerLag('kafka-lag', 'my-consumer-group', 1000),
    ]);
  }
}
```

`isHealthy` is up when every connection's producer is connected and every consumer group is running. Producers connect at startup; a failure only logs a warning, and each `isHealthy` call (or the next `send()`) starts a new connection attempt, so readiness recovers once the brokers are back. The result lists each part:

```json
{
  "kafka": {
    "status": "down",
    "producers": { "default": true },
    "consumers": { "default:orders-group": "failed" },
    "message": "Not ready: consumer group \"default:orders-group\" is failed"
  }
}
```

`checkBrokers(key, connection?)` and `checkConsumerLag(key, groupId, maxLag = 1000, connection?)` use one admin client per connection, connected on first use and kept between probes (closed on shutdown). Pass a connection name to probe a named connection. A topic whose latest offsets cannot be read is listed in `unreadableTopics` (and logged) instead of silently counting as lag 0.

Use `isHealthy` for a **readiness** probe. Do not restart the app (liveness) on a Kafka outage: librdkafka reconnects by itself and consumption resumes when the brokers are back, while the producer flag can stay `true` during an outage. `checkBrokers` probes broker reachability when you need it.

## OpenTelemetry Tracing

The library supports distributed tracing with OpenTelemetry, allowing you to trace messages from producer to consumer with the same trace ID.

### How It Works

1. **Producer**: When sending a message, the library creates a span and injects the trace context (W3C Trace Context format) into Kafka message headers
2. **Consumer**: When receiving a message, the library extracts the trace context from headers and creates a child span linked to the producer's trace
3. **Batch Consumer**: For batch processing, the first message's trace becomes the parent, and all other messages are added as span links

```
┌─────────────────┐                         ┌─────────────────┐
│  HTTP Request   │                         │  Consumer App   │
│  TraceID: abc   │                         │                 │
│  ┌───────────┐  │      Kafka Topic        │  ┌───────────┐  │
│  │  publish  │──┼─────────────────────────┼──│  process  │  │
│  │  span     │  │  Headers:               │  │  span     │  │
│  │           │  │  traceparent: 00-abc... │  │           │  │
│  └───────────┘  │                         │  └───────────┘  │
│                 │                         │  TraceID: abc   │
└─────────────────┘                         └─────────────────┘
```

### Prerequisites

1. Install OpenTelemetry packages:

```bash
npm install @opentelemetry/api @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
```

2. **IMPORTANT**: Initialize OpenTelemetry SDK **BEFORE** your NestJS app starts:

```typescript
// tracing.ts
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';

const sdk = new NodeSDK({
  resource: new Resource({
    [ATTR_SERVICE_NAME]: 'my-kafka-service',
  }),
  traceExporter: new OTLPTraceExporter({
    url: 'http://localhost:4318/v1/traces', // Jaeger/OTLP endpoint
  }),
});

sdk.start();
```

```typescript
// main.ts
import './tracing'; // MUST be first import
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  await app.listen(3000);
}
bootstrap();
```

### Important: Disable KafkaJS Auto-Instrumentation

If you're using `@opentelemetry/auto-instrumentations-node`, you **MUST** disable the KafkaJS auto-instrumentation to use this library's tracing. Otherwise, you'll get duplicate spans and the library's spans (with `consumer.group` attribute) won't be used.

```typescript
// tracing.ts
import { NodeSDK } from '@opentelemetry/sdk-node';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';

const sdk = new NodeSDK({
  instrumentations: [
    getNodeAutoInstrumentations({
      // Disable kafkajs auto-instrumentation
      '@opentelemetry/instrumentation-kafkajs': {
        enabled: false,
      },
    }),
  ],
  // ... other config
});

sdk.start();
```

### Enable Tracing

```typescript
// app.module.ts
import { Module } from '@nestjs/common';
import { KafkaModule, ConsumerModule } from '@loipv/nestjs-kafka';

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
      tracing: {
        enabled: true,                        // Required: enable tracing
        tracerName: 'my-kafka-service',       // Optional: custom tracer name
        tracerVersion: '1.0.0',               // Optional: custom tracer version
      },
    }),
    ConsumerModule.forRoot(),
  ],
})
export class AppModule {}
```

### Tracing Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable OpenTelemetry tracing |
| `tracerName` | string | `'@loipv/nestjs-kafka'` | Custom tracer name |
| `tracerVersion` | string | `'0.0.1'` | Custom tracer version |

### Span Names

Span names include the consumer group for easy identification in tracing UI:

| Type | Span Name Format | Example |
|------|------------------|---------|
| Producer | `{topic} publish` | `orders publish` |
| Consumer | `{groupId} {topic} process` | `order-group orders process` |
| Batch Consumer | `{groupId} {topic} process batch` | `order-group orders process batch` |

### Span Attributes (OpenTelemetry Semantic Conventions v1.24+)

**Producer Span:**

| Attribute | Example | Description |
|-----------|---------|-------------|
| `messaging.system` | `kafka` | Messaging system |
| `messaging.destination.name` | `orders` | Topic name |
| `messaging.operation.name` | `publish` | Operation name |
| `messaging.operation.type` | `publish` | Operation type |
| `messaging.destination.partition.id` | `0` | Partition (if specified) |
| `messaging.kafka.message.key` | `customer-123` | Message key (if present) |

**Consumer Span:**

| Attribute | Example | Description |
|-----------|---------|-------------|
| `messaging.system` | `kafka` | Messaging system |
| `messaging.destination.name` | `orders` | Topic name |
| `messaging.destination.partition.id` | `0` | Partition number |
| `messaging.operation.name` | `process` | Operation name |
| `messaging.operation.type` | `process` | Operation type |
| `messaging.kafka.offset` | `12345` | Message offset |
| `messaging.kafka.consumer.group` | `order-group` | Consumer group ID |
| `messaging.kafka.message.key` | `customer-123` | Message key (if present) |

**Batch Consumer Span (additional):**

| Attribute | Example | Description |
|-----------|---------|-------------|
| `messaging.batch.message_count` | `100` | Number of messages in batch |

### Batch Tracing with Links

For batch consumers (`batch: true`), messages from different requests/traces are processed together. The library handles this by:

1. **First message's trace** becomes the **parent** (for trace continuity)
2. **All other messages** are added as **span links** (shows relationship in tracing UI)

```
Request A (trace-A) ──publish──▶ Message 1 ──┐
Request B (trace-B) ──publish──▶ Message 2 ──┼──▶ Batch Consumer Span
Request C (trace-C) ──publish──▶ Message 3 ──┘      │
                                                     ├── Parent: trace-A
                                                     └── Links: [trace-B, trace-C]
```

This allows you to:
- Follow the full trace from the first message's request
- See all related traces via span links in your tracing UI (Jaeger, Zipkin, etc.)

### Trace Context Propagation

The library uses W3C Trace Context format for propagation via Kafka headers:

| Header | Format | Example |
|--------|--------|---------|
| `traceparent` | `{version}-{traceId}-{spanId}-{flags}` | `00-abc123...-def456...-01` |
| `tracestate` | Vendor-specific state | `vendor=value` |

### Troubleshooting

**No spans appearing:**
1. Ensure `tracing.enabled: true` in KafkaModule options
2. Ensure OpenTelemetry SDK is initialized **before** NestJS app starts
3. Ensure `@opentelemetry/api` is installed
4. If using auto-instrumentations, disable `@opentelemetry/instrumentation-kafkajs`

**Missing `messaging.kafka.consumer.group` attribute:**
- You're likely using `@opentelemetry/instrumentation-kafkajs` which creates its own spans
- Disable it and use this library's TracingService instead

**Duplicate spans:**
- Both auto-instrumentation and this library are creating spans
- Disable `@opentelemetry/instrumentation-kafkajs` in auto-instrumentations config

## API Reference

### Exports

```typescript
// Modules
export { KafkaModule, ConsumerModule } from '@loipv/nestjs-kafka';

// Decorators
export { Consumer, InjectKafkaClient } from '@loipv/nestjs-kafka';

// Services
export {
  KafkaClient,
  ConnectionBoundClient, // what @InjectKafkaClient('<name>') injects
  TracingService,
  KafkaHealthIndicator,
  ConsumerRegistryService, // startAll() with ConsumerModule.forRoot({ autoStart: false })
} from '@loipv/nestjs-kafka';

// Tokens and constants
export {
  DEFAULT_KAFKA_CONNECTION,
  CONSUMER_MODULE_OPTIONS,
  MAX_RETRY_DELAY_MS,
  getKafkaOptionsToken,
  getKafkaClientToken,
} from '@loipv/nestjs-kafka';

// Deserialization helpers
export {
  deserializeMessage,
  deserializeMessageValue,
  deserializeMessageKey,
  deserializeHeaders,
} from '@loipv/nestjs-kafka';

// Types
export type {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  KafkaOptionsFactory,
  ProducerConfig,
  CompressionCodec,
  TracingOptions,
  ConsumerModuleOptions,
  ConsumerOptions,
  ConsumerRetryOptions,
  DlqOptions,
  PartitionAssigner,
  ProducerMessage,
  SendOptions,
  KafkaMessagePayload,
  GroupedBatch,
} from '@loipv/nestjs-kafka';
```

## Migration Guide (from v1.x to v2.x)

v2.0.0 removes features that did not work as documented or added risk without benefit, fixes configurations that failed at startup, and changes how failed messages are retried.

### Fixed

- `KafkaModule.forRoot({ producer: { ... } })` no longer crashes at startup. `compression` accepts a codec name (`'gzip'`) or the old number (`1`).
- Producer options left `undefined` (for example an unset environment variable in `forRootAsync`) are ignored instead of making every send fail.
- A producer whose first connect fails (for example a DLQ cluster that is down) is rebuilt, so the next send reconnects once the broker is back.
- Several `KafkaModule.forRoot()` / `forRootAsync()` calls now share one connection registry, so a `@Consumer({ connection: 'analytics' })` finds a connection registered by a second `forRoot()`.
- A message being processed during shutdown is no longer acknowledged: it is redelivered on the next start instead of being lost.
- DLQ messages keep the original value and key bytes (binary payloads were corrupted by a UTF-8 conversion). Producer keys `0` and `''` are no longer sent as `null`.
- `ssl: { ca, cert, key, ... }` is applied (the object used to be reduced to `ssl: true`).
- `batchSize` above 32 takes effect (the client used to fetch at most 32 messages per call).
- When consumer startup fails, the consumers that did start and the producers are closed before the error surfaces, so a failed boot no longer keeps the process alive.
- `autoCommit: false` commits: offsets used to be stored but never committed, so the group re-read everything after each restart.
- A `ConsumerModule.forRoot()` default (including each `retry` field) is no longer hidden by an explicit `undefined` in `@Consumer()` options (for example an unset environment variable).
- Consumer discovery no longer reads provider getters (it could trigger their side effects).
- Producers of several connections disconnect in parallel on shutdown.
- The published type declarations keep their JSDoc (descriptions and defaults show in the editor).
- A consumer provider also exposed through `useExisting` no longer fails startup with "already has a handler for topic".
- `@Consumer` methods declared on a base class are discovered (they were silently ignored). An application that registers both the base class and a subclass as providers now fails at startup with "already has a handler" (the subclass used to be ignored); register only one. Request-scoped and transient consumer providers are skipped with a warning instead of registering a handler without its dependencies.
- Consumers always stop before producers disconnect, whatever the import order of `KafkaModule` and `ConsumerModule`.
- A partially failed DLQ send only re-sends the messages that did not arrive.
- A synchronous commit (`autoCommit: false`) waits at most 10s (a hung commit used to block the worker and `app.close()`).

### Changed behaviour

| Before (v1) | Now (v2) |
|---|---|
| A failing handler was re-run inside the same call (`sleep` between attempts); long retries blocked every partition and could get the member evicted | The partition is paused, the error rethrown, and the message redelivered after the delay; other partitions keep flowing. Each retry logs an extra line from the Kafka client |
| Retry delays capped at 30s; `retry.retries` / `initialRetryTime` / `maxRetryTime` were also copied into the client's request backoff | Capped by `retry.maxRetryTime` (default 30s), DLQ backoff included. `retry` configures handler retries only; client backoff is `KafkaModule.forRoot({ retry })` or `rdkafka` |
| A failed DLQ send was redelivered immediately | It is retried with the backoff delay; `dlq.sendTimeout` (default 30s, must be positive) bounds each send |
| Batch consumers decided on the DLQ with `retry.retries` and sent messages one by one | Batch consumers use `dlq.maxRetries` like single-message consumers and dead-letter a chunk in one send |
| A batch was committed once, after all its chunks | Each successful chunk is committed; a failing chunk is redelivered without the earlier ones |
| Two `@Consumer`s for the same topic in one group: the last one silently won | Fails at startup |
| Idempotency keys were global and unbounded; a batch also recorded keys of dead-lettered or skipped chunks | Scoped per connection, group and topic; at most `idempotencyMaxKeys` (a positive integer, default 100000) kept; recorded only after the handler succeeds |
| A throwing `idempotencyKey` extractor redelivered the message in a tight loop | It is handled like a handler failure (pause, backoff, skip/DLQ) |
| Consumers started in `onModuleInit` | Consumers start in `onApplicationBootstrap`, after every provider's `onModuleInit`; `ConsumerModule.forRoot({ autoStart: false })` leaves starting to `ConsumerRegistryService.startAll()` |
| The consumer registry stopped in `onApplicationShutdown` | It stops in `onModuleDestroy`, before producers disconnect |
| A DLQ chunk was sent in one `sendBatch`; a partial failure re-sent the whole chunk | One send per message; a retry sends only what did not arrive. `DlqService.handleFailure()` / `sendToDlq()` take an optional `sent` set |
| Health probes created and connected an admin client per call, on the default connection only | One shared admin per connection; `checkBrokers` / `checkConsumerLag` take an optional connection name; lag details list `unreadableTopics` |
| `isHealthy` checked one producer, connected on the first send | Producers connect at startup; `isHealthy` covers every producer and every consumer group, with per-part details (`connected` is replaced by `producers` / `consumers`) |
| `ssl` object silently ignored | Applied as librdkafka properties; unknown keys fail at startup. New `rdkafka` option passes raw librdkafka properties (except `enable.auto.commit`: use `autoCommit`) |
| `DlqService.handleFailure(message, ...)` / `sendToDlq(message, ...)`; `handleFailure` rejected on a failed send | Both take `messages: KafkaMessage[]`; `handleFailure` never rejects (it returns a retry verdict) |
| `IdempotencyService` methods took `(message, keyExtractor)` | They take a `namespace` argument as well |
| A second `forRoot()` with an existing connection name was ignored with a warning | It fails at startup when its options differ (the same options twice are still accepted) |
| `retry.initialRetryTime` / `retry.maxRetryTime` / `dlq.sendTimeout` / `dlq.retryDelay` above 2147483647 ms fired after 1 ms (a DLQ send then always timed out); `0`, a negative value or a multiplier below 1 was accepted (near-tight redelivery loop) | Delays must be positive and at most 2147483647 ms; `retry.multiplier` / `dlq.retryBackoffMultiplier` must be finite and >= 1; other values fail at startup |
| Trace context was written and read as hand-built `traceparent` / `tracestate` headers | It goes through the application's registered OpenTelemetry propagator (W3C by default; B3 and `baggage` work). Without a registered propagator no trace headers are written |

### Removed

| Removed | Replacement |
|---|---|
| `dlq.retry` (DLQ auto-retry consumer), `finalDlqTopic`, `DlqRetryOptions` | Add a normal `@Consumer` on the DLQ topic |
| Circuit breaker on DLQ sends (`DlqService.getCircuitState()`, `resetCircuit()`) | None. A failed DLQ send is redelivered and retried; nothing is dropped |
| DLQ metrics (`getMetrics()`) | None |
| `KafkaClient.sendQueued()` | `send()` / `sendBatch()`; librdkafka batches internally (`linger.ms`) |
| `KafkaModule.forRootMultiple([a, b])` | `KafkaModule.forRoot(a), KafkaModule.forRoot(b)` |
| `KAFKA_MODULE_OPTIONS` token (ambiguous with several connections) | `@Inject(getKafkaOptionsToken(name))` (`getKafkaOptionsToken()` for the default connection) |
| `KafkaClient.forConnection(name)` | `send(topic, message, { connection: name })` or `@InjectKafkaClient(name)` |
| `SendOptionsWithConnection` | `SendOptions` (now `{ connection?: string }`); `ConnectionBoundClient` methods take no options |
| `backPressureThreshold`, `maxQueueSize`, `maxConcurrency`, and the `PressureState` / `PressureManagerOptions` types | None (they had no effect) |
| `batchTimeout` | None. A batch is the fetched messages, split by `batchSize` |
| Consumer groups mixing batch and non-batch consumers | Use separate `groupId`s; mixing now fails at startup |
| Consumer auto-restart | None needed: the client never stops on handler or broker errors. An error from `consumer.run()` at startup now fails application startup, like connect and subscribe errors already did |
| `ConsumerModule.clearConsumers()`, `DlqService.clearRetryState()` | None (they were no-ops) |
| `ConsumerDiscoveryService.registerHandler()` / `clearConsumers()` | Decorate a provider method with `@Consumer()` |
| `getKafkaCoreToken()`, `KafkaModuleOptions.defaultConsumerGroupId`, function-valued `brokers`, `ConsumerOptions.topic` | None (unused or rejected by the client) |
| `new TracingService(options)` | `new TracingService()` then `configure(options.tracing)`; only matters if you construct it yourself (Nest does this for you) |

## Migration Guide (from v0.x to v1.x)

v1.0.0 introduces a **breaking change**: migrating from `kafkajs` to `@confluentinc/kafka-javascript` for better performance and official Confluent support.

### Why Migrate?

- **Performance**: confluent-kafka-javascript is built on librdkafka (C library) - significantly better performance
- **Commercial Support**: Official Confluent support
- **Active Development**: More active development compared to kafkajs

### Breaking Changes

#### 1. Install Dependencies

```bash
# Remove kafkajs, add confluent-kafka-javascript
npm uninstall kafkajs
npm install @confluentinc/kafka-javascript
```

#### 2. Update Imports

```typescript
// Before (v0.x)
import { KafkaMessage } from 'kafkajs';

// After (v1.x)
import { KafkaJS } from '@confluentinc/kafka-javascript';
type KafkaMessage = KafkaJS.KafkaMessage;
```

#### 3. Producer Options Moved to Module Level

`acks`, `timeout`, and `compression` are now configured at the producer level, not per-send call.

```typescript
// Before (v0.x) - per-send options
await kafka.send('topic', message, {
  acks: -1,
  timeout: 30000,
  compression: 1,
});

// After (v1.x) - producer-level options
KafkaModule.forRoot({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  producer: {
    acks: -1,
    timeout: 30000,
    compression: 1,
  },
});

await kafka.send('topic', message); // No acks/timeout/compression
```

#### 4. autoCommitThreshold Removed

`autoCommitThreshold` is not supported in confluent-kafka-javascript. Remove this option from your consumer configuration.

```typescript
// Before (v0.x)
@Consumer('topic', {
  autoCommitThreshold: 100, // Not supported
})

// After (v1.x)
@Consumer('topic', {
  // autoCommitThreshold removed - use autoCommitInterval instead
  autoCommitInterval: 5000,
})
```

#### 5. Retry Options Changes

The following retry options have been removed as they are not supported in confluent-kafka-javascript:

- `retry.restartOnFailure` - Consumer restart control is handled by the library internally
- `retry.factor` - Use `retry.multiplier` for exponential backoff

```typescript
// Before (v0.x)
@Consumer('topic', {
  retry: {
    restartOnFailure: false,  // Removed
    factor: 0.2,              // Removed
  },
})

// After (v1.x)
@Consumer('topic', {
  retry: {
    retries: 3,
    multiplier: 2,            // Use multiplier for backoff
  },
})
```

#### 6. Platform Requirements

See [Platform Support](#platform-support) for the supported platforms and Node.js versions.

### No Changes Required

The following features work the same way:
- `@Consumer()` decorator syntax
- `KafkaClient.send()`, `sendBatch()`, `sendMultiTopicBatch()`
- DLQ configuration and retry
- OpenTelemetry tracing
- Health checks
- Multi-connection support
- Batch processing and key grouping
- Idempotency

## License

MIT
