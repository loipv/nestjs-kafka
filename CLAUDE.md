# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `@loipv/nestjs-kafka`, a production-ready NestJS module for Kafka client and consumer functionality built on top of confluent-kafka-javascript (librdkafka). The library provides enterprise-grade features including intelligent batch processing, idempotency guarantees, key-based grouping, and automatic pressure management.

## Commands

```bash
# Build
npm run build

# Testing
npm test               # Run unit tests
npm run test:watch     # Watch mode
npm run test:cov       # With coverage

# Run a single test file
npx jest path/to/file.spec.ts

# Linting & Formatting
npm run lint           # ESLint with auto-fix
npm run format         # Prettier

# Publish to npm
npm publish --access public
```

## Architecture

```
lib/
├── index.ts                         # Main barrel export
├── kafka.module.ts                  # KafkaModule - Infrastructure only
├── consumer.module.ts               # ConsumerModule - Consumer logic
├── interfaces/
│   ├── kafka-module-options.interface.ts
│   ├── consumer-options.interface.ts
│   └── message.interface.ts
├── decorators/
│   ├── consumer.decorator.ts        # @Consumer() method decorator
│   ├── inject-kafka-client.decorator.ts # @InjectKafkaClient() for named connections
│   └── constants.ts
├── services/
│   ├── kafka-core.service.ts        # [KafkaModule] Connection management
│   ├── kafka-client.service.ts      # [KafkaModule] Producer (send/sendBatch)
│   ├── tracing.service.ts           # [KafkaModule] OpenTelemetry tracing
│   ├── consumer-registry.service.ts # [ConsumerModule] Consumer lifecycle
│   ├── batch-processor.service.ts   # [ConsumerModule] Batch accumulation
│   ├── idempotency.service.ts       # [ConsumerModule] Duplicate prevention
│   ├── pressure-manager.service.ts  # [ConsumerModule] Back pressure
│   ├── dlq.service.ts               # [ConsumerModule] Dead Letter Queue
│   ├── dlq-retry.service.ts         # [ConsumerModule] DLQ auto-retry
│   ├── dlq-metrics.service.ts       # [ConsumerModule] DLQ metrics tracking
│   └── circuit-breaker.service.ts   # [ConsumerModule] Circuit breaker for DLQ
├── discovery/
│   └── consumer-discovery.service.ts # [ConsumerModule] Auto-discover @Consumer
└── health/
    └── kafka-health-indicator.ts    # [KafkaModule] Health checks
```

### Module Separation

- **KafkaModule**: Infrastructure (connections, producer, health check)
- **ConsumerModule**: Consumer logic (discovery, registry, batch, DLQ, etc.)

### Key Components

- **KafkaModule**: Root module with `forRoot()`, `forRootAsync()`, and `forRootMultiple()` for configuration
  - Supports multi-connection setup with named connections
- **ConsumerModule**: Auto-discovers and registers consumer methods on app startup
  - Use `forRoot(options?)` in app module with optional default options
  - All `@Consumer()` decorated methods are automatically discovered via NestJS DiscoveryService
  - Default options are merged with `@Consumer` decorator options (decorator takes precedence)
- **KafkaClient**: Producer service with `send()`, `sendBatch()`, `sendQueued()`, `sendMultiTopicBatch()` methods
- **@Consumer() decorator**: Method decorator to define topic consumers with batch/pressure/DLQ options
- **@InjectKafkaClient() decorator**: Inject named connection clients in services
- **KafkaHealthIndicator**: Health checks for Kafka connections

### Features

- Multi-connection support (connect to multiple Kafka clusters simultaneously)
- Intelligent batch processing with configurable size and timeout
- Key-based message grouping for ordered processing within batches
- Back pressure management (pause/resume consumption)
- In-memory idempotency with TTL
- Dead Letter Queue with exponential backoff retry and auto-retry from DLQ
- Circuit breaker for DLQ operations
- DLQ metrics tracking
- **OpenTelemetry tracing** (distributed tracing across produce → consume)
- Graceful shutdown with proper cleanup

## Important Behavior Notes

### Retry Mechanism Without DLQ

When **NOT using DLQ**, the library implements an in-memory retry mechanism with exponential backoff:

1. **Retry with delay**: Message will be retried up to `retry.retries` times (default: 3) with exponential backoff
2. **After max retries exceeded**:
   - By default (`skipMessageOnMaxRetries: false`): Error is **thrown**, which may cause consumer to stop/restart
   - If `skipMessageOnMaxRetries: true`: Message is **skipped** and offset is committed to avoid blocking the consumer

**Configuration options:**
- `skipMessageOnMaxRetries: false` (default): Throw error to ensure no message is silently dropped
- `skipMessageOnMaxRetries: true`: Skip message to prevent consumer blocking (useful for multi-topic consumers)

**Example:**
```typescript
// Default behavior: Throw error after max retries
@Consumer('orders', {
  retry: {
    retries: 3,
    initialRetryTime: 1000,
    multiplier: 2,
    // skipMessageOnMaxRetries: false (default)
  },
})
async handleOrder(message: KafkaMessage) {
  // If this fails 3 times, error is thrown
}

// Skip message to avoid blocking (for multi-topic consumers)
@Consumer('non-critical-logs', {
  retry: {
    retries: 5,
    skipMessageOnMaxRetries: true, // Skip to avoid blocking
  },
})
async handleLogs(message: KafkaMessage) {
  // If this fails 5 times, message is skipped
}
```

### Retry Mechanism With DLQ

When **using DLQ**, failed messages are sent to the DLQ topic after max retries. The message is NOT skipped or dropped.

### OpenTelemetry Tracing

The library supports distributed tracing with OpenTelemetry. When enabled, trace context is:
1. **Injected** into Kafka message headers when producing (W3C Trace Context format)
2. **Extracted** from headers when consuming, linking producer and consumer spans

**Enable tracing:**
```typescript
KafkaModule.forRoot({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  tracing: {
    enabled: true,
    tracerName: '@loipv/nestjs-kafka', // Optional
  },
})
```

**Prerequisites:**
- Install `@opentelemetry/api` (peer dependency)
- Set up OpenTelemetry SDK in your application

**Trace flow:**
```
Producer App                    Consumer App
┌─────────────────┐            ┌─────────────────┐
│  Span: publish  │ ────────── │  Span: process  │
│  TraceID: abc   │   Kafka    │  TraceID: abc   │
└─────────────────┘  Headers   └─────────────────┘
```

## Tech Stack

- NestJS 11
- @confluentinc/kafka-javascript (librdkafka-based, KafkaJS-compatible API)
- TypeScript (ES2023 target, CommonJS module)
- Jest for testing
- ESLint + Prettier for code style

## Important Notes for confluent-kafka-javascript

1. **Producer options** (`acks`, `compression`, `timeout`) are configured at producer level in `KafkaModule.forRoot()`, NOT per-send call
2. **heartbeat()** is automatic - no manual calls needed
3. **consumer.stop()** is not supported - use `disconnect()` directly
4. **autoCommitThreshold** is not supported - use `autoCommitInterval` instead
5. **Platform support**: Linux (x64/arm64), macOS (arm64), Windows (x64), Node.js 18-22

## Usage Example

```typescript
// app.module.ts (Root Module)
@Module({
  imports: [
    // Infrastructure module (producer, connections)
    KafkaModule.forRoot({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    }),
    // Consumer module with default options (applied to all @Consumer decorators)
    ConsumerModule.forRoot({
      partitionAssigners: ['cooperative-sticky'],  // Default for all consumers
      allowAutoTopicCreation: true,                // Auto-create topics
      sessionTimeout: 30000,
      // These defaults are used when @Consumer doesn't specify them
    }),
    OrderModule,  // Feature module
  ],
})
export class AppModule {}

// order/order.module.ts (Feature Module)
@Module({
  // No need to import ConsumerModule — consumers are auto-discovered!
  providers: [OrderConsumer, OrderService],
})
export class OrderModule {}

// order/order.consumer.ts
@Injectable()
export class OrderConsumer {
  @Consumer('orders')
  async handleOrder(message: KafkaMessage) {
    // Process single message
  }

  @Consumer('orders-batch', {
    batch: true,
    batchSize: 100,
    groupByKey: true,
    dlq: { topic: 'orders-dlq', maxRetries: 3 },
  })
  async handleBatch(messages: KafkaMessage[]) {
    // Process batch
  }
}

// order/order.service.ts
@Injectable()
export class OrderService {
  constructor(private kafka: KafkaClient) {}

  async createOrder(order: Order) {
    await this.kafka.send('orders', {
      key: order.customerId,
      value: order,
    });
  }
}
```
