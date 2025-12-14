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
import { Module } from '@nestjs/common';
import { KafkaModule, ConsumerModule } from '@loipv/nestjs-kafka';
import { OrderConsumer } from './order.consumer';

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    }),
    ConsumerModule,
  ],
  providers: [OrderConsumer],
})
export class AppModule {}
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

### All Consumer Options

```typescript
interface ConsumerOptions {
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
  };

  // Commit settings
  autoCommit?: boolean;           // Default: true
  autoCommitInterval?: number;
  fromBeginning?: boolean;        // Default: false
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

```typescript
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

// Health
export { KafkaHealthIndicator } from './health/kafka-health-indicator';

// Interfaces
export {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  ConsumerOptions,
  DlqOptions,
  ProducerMessage,
  SendOptions,
  GroupedBatch,
} from './interfaces';
```

## License

MIT
