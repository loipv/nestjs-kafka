# @loipv/nestjs-kafka

A production-ready NestJS module for Kafka client and consumer functionality built on top of [KafkaJS](https://kafka.js.org/). This library provides enterprise-grade features including intelligent batch processing, idempotency guarantees, key-based grouping, and automatic pressure management.

## Features

- **Producer (KafkaClient)**: High-performance Kafka producer with `send()`, `sendBatch()`, `sendQueued()` methods
- **Consumer**: Method decorator-based consumer with auto-discovery
- **Batch Processing**: Intelligent batching with configurable size and timeout
- **Key-Based Grouping**: Group messages by key within batches for ordered processing
- **Back Pressure**: Automatic pause/resume when consumers are overwhelmed
- **Idempotency**: In-memory duplicate prevention with TTL
- **Dead Letter Queue (DLQ)**: Automatic retry with exponential backoff
- **Health Checks**: Integration with `@nestjs/terminus`
- **Graceful Shutdown**: Proper cleanup on application shutdown

## Installation

```bash
npm install @loipv/nestjs-kafka kafkajs
```

### Peer Dependencies

Make sure you have the following peer dependencies installed:

```bash
npm install @nestjs/common @nestjs/core @nestjs/terminus reflect-metadata rxjs
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
    ConsumerModule.forRoot(),  // Initialize in root module
    OrderModule,
  ],
})
export class AppModule {}
```

```typescript
// order/order.module.ts (Feature Module)
import { Module } from '@nestjs/common';
import { ConsumerModule } from '@loipv/nestjs-kafka';
import { OrderConsumer } from './order.consumer';

@Module({
  imports: [
    ConsumerModule.forFeature([OrderConsumer]),  // Register consumers
  ],
  providers: [OrderConsumer],  // Must also be in providers
})
export class OrderModule {}
```

### 2. Create a Consumer

```typescript
import { Injectable } from '@nestjs/common';
import { Consumer } from '@loipv/nestjs-kafka';
import { KafkaMessage } from 'kafkajs';

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
    ConsumerModule,
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
    ConsumerModule,
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

### Retry & Restart on Failure

Control consumer restart behavior when errors occur:

```typescript
// Disable restart on failure
@Consumer('critical-topic', {
  retry: {
    restartOnFailure: false,
  },
})
async handleCritical(message: KafkaMessagePayload) {
  // Consumer will NOT restart if this throws
}

// Custom restart logic
@Consumer('orders', {
  retry: {
    retries: 10,
    maxRetryTime: 60000,
    restartOnFailure: async (error) => {
      // Don't restart on authentication errors
      if (error.message.includes('authentication')) {
        return false;
      }
      return true; // Restart for other errors
    },
  },
})
async handleOrders(message: KafkaMessagePayload) {
  // Process order
}
```

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

  // Retry & restart on failure
  retry?: {
    retries?: number;             // Default: 5
    maxRetryTime?: number;        // Default: 30000
    initialRetryTime?: number;    // Default: 300
    factor?: number;              // Default: 0.2
    multiplier?: number;          // Default: 2
    restartOnFailure?: boolean | ((error: Error) => Promise<boolean>);
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

### Send Options

```typescript
await kafka.send('topic', message, {
  acks: -1,        // -1 (all), 0 (none), 1 (leader only)
  timeout: 30000,
  compression: 1,  // 0=None, 1=GZIP, 2=Snappy, 3=LZ4, 4=ZSTD
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
export { DlqMetricsService } from './services/dlq-metrics.service';
export { CircuitBreakerService } from './services/circuit-breaker.service';

// Health
export { KafkaHealthIndicator } from './health/kafka-health-indicator';

// Interfaces
export {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  ConsumerOptions,
  DlqOptions,
  DlqRetryOptions,
  ProducerMessage,
  SendOptions,
  GroupedBatch,
} from './interfaces';
```

## License

MIT
