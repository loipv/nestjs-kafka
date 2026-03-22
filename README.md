# @loipv/nestjs-kafka

A production-ready NestJS module for Kafka client and consumer functionality built on top of [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript). This library provides enterprise-grade features including intelligent batch processing, idempotency guarantees, key-based grouping, and automatic pressure management.

> **Note:** Starting from v1.0.0, this library uses `@confluentinc/kafka-javascript` instead of `kafkajs` for better performance and official Confluent support. See [Migration Guide](#migration-guide-from-v0x-to-v1x) for upgrade instructions.

## Features

- **Producer (KafkaClient)**: High-performance Kafka producer with `send()`, `sendBatch()`, `sendQueued()` methods
- **Consumer**: Method decorator-based consumer with auto-discovery
- **Batch Processing**: Intelligent batching with configurable size and timeout
- **Key-Based Grouping**: Group messages by key within batches for ordered processing
- **Back Pressure**: Automatic pause/resume when consumers are overwhelmed
- **Idempotency**: In-memory duplicate prevention with TTL
- **Dead Letter Queue (DLQ)**: Automatic retry with exponential backoff
- **OpenTelemetry Tracing**: Distributed tracing across produce → consume with same trace ID
- **Health Checks**: Integration with `@nestjs/terminus`
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
- **Node.js**: 18, 20, 21, 22

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
import { Consumer } from '@loipv/nestjs-kafka';
import { KafkaMessage } from '@confluentinc/kafka-javascript/kafkajs';

@Injectable()
export class OrderConsumer {
  @Consumer('orders')
  async handleOrder(message: KafkaMessage) {
    const order = JSON.parse(message.value.toString());
    console.log('Processing order:', order);
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

  // Optional - SSL/SASL
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
async handleOrder(message: KafkaMessage) {}

// This consumer overrides partitionAssigners but keeps sessionTimeout from defaults
@Consumer('events', { partitionAssigners: ['roundrobin'] })
async handleEvent(message: KafkaMessage) {}
```

**ConsumerModuleOptions:**

| Option | Type | Description |
|--------|------|-------------|
| `partitionAssigners` | `PartitionAssigner[]` | Default partition assignment strategies |
| `allowAutoTopicCreation` | `boolean` | Auto-create topics if not exist |
| `sessionTimeout` | `number` | Session timeout in ms |
| `heartbeatInterval` | `number` | Heartbeat interval in ms |
| `rebalanceTimeout` | `number` | Rebalance timeout in ms |
| `autoCommit` | `boolean` | Enable auto commit |
| `autoCommitInterval` | `number` | Auto commit interval in ms |
| `fromBeginning` | `boolean` | Start consuming from beginning |
| `retry` | `ConsumerRetryOptions` | Default retry options |

### Multi-Connection (Multiple Kafka Clusters)

Connect to multiple Kafka clusters simultaneously:

```typescript
// Option 1: Multiple forRoot() calls
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

// Option 2: forRootMultiple() for cleaner setup
@Module({
  imports: [
    KafkaModule.forRootMultiple([
      {
        name: 'default',
        clientId: 'my-app',
        brokers: ['primary-kafka:9092'],
      },
      {
        name: 'analytics',
        clientId: 'my-app-analytics',
        brokers: ['analytics-kafka:9092'],
      },
    ]),
    ConsumerModule.forRoot(),
  ],
})
export class AppModule {}
```

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

// Method 2: Use forConnection() fluent API
@Injectable()
export class AnotherService {
  constructor(private readonly kafka: KafkaClient) {}

  async sendToAnalytics() {
    const analyticsClient = this.kafka.forConnection('analytics');
    await analyticsClient.send('events', { value: data });
  }

  async sendWithOptions() {
    // Or specify connection in options
    await this.kafka.send('orders', { value: data }, { connection: 'analytics' });
  }
}
```

## Consumer Options

### Basic Consumer

```typescript
@Consumer('topic-name')
async handleMessage(message: KafkaMessage) {
  // Process single message
}
```

### Batch Consumer

```typescript
@Consumer('orders', {
  batch: true,
  batchSize: 100,        // Max messages per batch (default: 100)
  batchTimeout: 5000,    // Max wait time in ms (default: 5000)
})
async handleBatch(messages: KafkaMessage[]) {
  // Process batch of messages
}
```

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
  },
})
async handlePayment(message: KafkaMessage) {
  // If this throws, message will be retried then sent to DLQ
}
```

### DLQ with Auto-Retry

Automatically consume messages from DLQ and re-publish them to the original topic after a delay:

```typescript
@Consumer('payments', {
  dlq: {
    topic: 'payments-dlq',
    maxRetries: 3,
    retry: {
      enabled: true,           // Enable auto DLQ consumption
      maxRetries: 5,           // Max retries from DLQ
      delay: 60000,            // Wait 1 minute before re-publishing
      backoffMultiplier: 2,    // Exponential backoff
      finalDlqTopic: 'payments-dlq-final', // Optional: final dead letter
      fromBeginning: false,    // Start from latest (default)
      groupId: 'custom-dlq-group', // Optional: custom consumer group
    },
  },
})
async handlePayment(message: KafkaMessagePayload) {
  // Process payment...
}
```

**DLQ Retry Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable auto DLQ consumption |
| `maxRetries` | number | `3` | Max retries from DLQ before final dead letter |
| `delay` | number | `60000` | Delay before re-publishing (ms) |
| `backoffMultiplier` | number | `2` | Exponential backoff multiplier |
| `finalDlqTopic` | string | - | Topic for messages that exceed max retries |
| `fromBeginning` | boolean | `false` | Start consuming from beginning of DLQ |
| `groupId` | string | `${dlqTopic}-retry-consumer` | Consumer group ID |

**DLQ Retry Flow:**
1. Message fails in handler → sent to DLQ topic
2. DLQ retry consumer picks up message
3. Waits with exponential backoff delay
4. Calls original handler again
5. If still fails after max DLQ retries → sent to `finalDlqTopic` or dropped

**DLQ Headers:**

| Header | Description |
|--------|-------------|
| `x-dlq-original-topic` | Original topic name |
| `x-dlq-handler-retry-count` | Retries before sent to DLQ |
| `x-dlq-timestamp` | Timestamp when sent to DLQ |
| `x-dlq-error-message` | Error message |
| `x-dlq-reprocess-count` | Reprocess attempts from DLQ |
| `x-dlq-reprocess-timestamp` | Timestamp of reprocess |
| `x-final-dlq-reason` | Reason sent to final DLQ |

### Circuit Breaker

The DLQ system includes a circuit breaker to prevent flooding DLQ when the system is unhealthy:

```typescript
import { CircuitBreakerService, DlqService } from '@loipv/nestjs-kafka';

@Injectable()
export class MonitoringService {
  constructor(
    private readonly circuitBreaker: CircuitBreakerService,
    private readonly dlqService: DlqService,
  ) {}

  getCircuitStates() {
    return this.circuitBreaker.getAllStates();
  }

  resetCircuit(dlqTopic: string) {
    this.dlqService.resetCircuit(dlqTopic);
  }
}
```

**Circuit States:**
- `CLOSED`: Normal operation
- `OPEN`: DLQ blocked (failure threshold exceeded)
- `HALF_OPEN`: Testing recovery

### DLQ Metrics

Track DLQ operations with the metrics service:

```typescript
import { DlqMetricsService } from '@loipv/nestjs-kafka';

@Injectable()
export class MonitoringService {
  constructor(private readonly dlqMetrics: DlqMetricsService) {}

  @Get('metrics/dlq')
  getMetrics() {
    return this.dlqMetrics.getMetrics();
    // Returns:
    // {
    //   global: { handlerRetries, messagesSentToDlq, reprocessAttempts, ... },
    //   byTopic: { 'orders': { handlerRetries, sentToDlq, ... } }
    // }
  }
}
```

### Consumer with Idempotency

```typescript
@Consumer('events', {
  idempotencyKey: (msg) => msg.headers?.['event-id']?.toString(),
  idempotencyTtl: 3600000, // 1 hour
})
async handleEvent(message: KafkaMessage) {
  // Duplicate messages (same event-id) will be skipped
}
```

### Consumer with Back Pressure

```typescript
@Consumer('high-volume', {
  batch: true,
  backPressureThreshold: 80, // Pause at 80% capacity
  maxQueueSize: 1000,
})
async handleHighVolume(messages: KafkaMessage[]) {
  // Consumer will auto-pause when overwhelmed
}
```

### Disabled Consumer

```typescript
@Consumer('orders', {
  disabled: true, // Consumer will be skipped during registration
})
async handleOrder(message: KafkaMessage) {
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
async handleOrder(message: KafkaMessage) {
  // Process order
}

// Multiple strategies (first one is primary)
@Consumer('events', {
  partitionAssigners: ['roundrobin', 'range'],
})
async handleEvent(message: KafkaMessage) {
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
async handleBinary(message: KafkaMessage) {
  // message.value is Buffer
  const raw = message.value.toString('hex');
}
```

### Retry Mechanism (Without DLQ)

When **NOT using DLQ**, the library implements an in-memory retry mechanism with exponential backoff:

```typescript
@Consumer('orders', {
  retry: {
    retries: 3,                    // Default: 3 retries
    initialRetryTime: 1000,        // Default: 1000ms
    multiplier: 2,                 // Default: 2 (exponential backoff)
    skipMessageOnMaxRetries: false, // Default: false (throw error)
  },
})
async handleOrders(message: KafkaMessagePayload) {
  // If this fails, it will retry: 1s, 2s, 4s delays
  // After 3 retries, error is thrown (consumer may stop/restart)
}

// Skip message to avoid blocking consumer (useful for multi-topic consumers)
@Consumer('non-critical-logs', {
  retry: {
    retries: 5,
    skipMessageOnMaxRetries: true, // Skip message after max retries
  },
})
async handleLogs(message: KafkaMessagePayload) {
  // If this fails 5 times, message is skipped (offset committed)
  // Consumer continues processing next message
}
```

**Important Notes:**
- **Default behavior** (`skipMessageOnMaxRetries: false`): Error is thrown after max retries, ensuring no message is silently dropped
- **Skip mode** (`skipMessageOnMaxRetries: true`): Message is skipped after max retries to prevent consumer blocking
- **With DLQ**: Messages are sent to DLQ topic after max retries (recommended approach)

**When to use skip mode:**
- Multi-topic consumers where one failing topic shouldn't block others
- Non-critical messages that can be safely dropped
- Development/debugging environments

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
  groupId?: string;
  sessionTimeout?: number;      // Default: 30000
  heartbeatInterval?: number;   // Default: 3000
  rebalanceTimeout?: number;

  // Batch processing
  batch?: boolean;
  batchSize?: number;           // Default: 100
  batchTimeout?: number;        // Default: 5000
  groupByKey?: boolean;

  // Pressure management
  backPressureThreshold?: number; // Default: 80
  maxQueueSize?: number;          // Default: 1000

  // Idempotency
  idempotencyKey?: (message: KafkaMessage) => string | undefined;
  idempotencyTtl?: number;        // Default: 3600000 (1 hour)

  // Dead Letter Queue
  dlq?: {
    topic: string;
    maxRetries?: number;          // Default: 3
    retryDelay?: number;          // Default: 1000
    retryBackoffMultiplier?: number; // Default: 2
    retry?: {                     // DLQ auto-retry options
      enabled?: boolean;
      maxRetries?: number;
      delay?: number;
      backoffMultiplier?: number;
      finalDlqTopic?: string;
    };
  };

  // Commit settings
  autoCommit?: boolean;           // Default: true
  autoCommitInterval?: number;
  fromBeginning?: boolean;        // Default: false

  // Partition assignment strategy
  partitionAssigners?: PartitionAssigner[];  // 'roundrobin' | 'range' | 'cooperative-sticky'

  // Retry options
  retry?: {
    retries?: number;             // Default: 5
    maxRetryTime?: number;        // Default: 30000
    initialRetryTime?: number;    // Default: 300
    multiplier?: number;          // Default: 2 (exponential backoff)
    skipMessageOnMaxRetries?: boolean; // Default: false (throw error after max retries)
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

// Queue message for auto-batching
await kafka.sendQueued('topic', { value: 'message' });
```

### Producer Options

> **Note:** In v1.0.0+, `acks`, `timeout`, and `compression` are configured at the producer level in `KafkaModule.forRoot()`, not per-send call.

```typescript
// Configure producer options at module level
KafkaModule.forRoot({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  producer: {
    acks: -1,        // -1 (all), 0 (none), 1 (leader only)
    timeout: 30000,
    compression: 1,  // 0=None, 1=GZIP, 2=Snappy, 3=LZ4, 4=ZSTD
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

> **Note:** To use `KafkaHealthIndicator` with full Terminus integration, you must import `TerminusModule`. Without it, the health indicator will use a fallback implementation that returns plain objects.

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
export { KafkaModule } from './kafka.module';
export { ConsumerModule } from './consumer.module';

// Decorators
export { Consumer } from './decorators';

// Services
export { KafkaClient } from './services/kafka-client.service';
export { TracingService } from './services/tracing.service';
export { DlqMetricsService } from './services/dlq-metrics.service';
export { CircuitBreakerService } from './services/circuit-breaker.service';

// Health
export { KafkaHealthIndicator } from './health/kafka-health-indicator';

// Interfaces
export {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  TracingOptions,
  ConsumerModuleOptions,
  ConsumerOptions,
  DlqOptions,
  DlqRetryOptions,
  ProducerMessage,
  SendOptions,
  GroupedBatch,
} from './interfaces';
```

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
import { KafkaMessage } from '@confluentinc/kafka-javascript/kafkajs';
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

confluent-kafka-javascript only supports:
- **Linux**: x64, arm64
- **macOS**: arm64 (Apple Silicon)
- **Windows**: x64
- **Node.js**: 18, 20, 21, 22

### No Changes Required

The following features work the same way:
- `@Consumer()` decorator syntax
- `KafkaClient.send()`, `sendBatch()`, `sendMultiTopicBatch()`, `sendQueued()`
- DLQ configuration and retry
- OpenTelemetry tracing
- Health checks
- Multi-connection support
- Batch processing and key grouping
- Idempotency and back pressure

## License

MIT
