# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `@loipv/nestjs-kafka`, a production-ready NestJS module for Kafka client and consumer functionality built on top of kafkajs. The library provides enterprise-grade features including intelligent batch processing, idempotency guarantees, key-based grouping, and automatic pressure management.

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
│   └── constants.ts
├── services/
│   ├── kafka-core.service.ts        # [KafkaModule] Connection management
│   ├── kafka-client.service.ts      # [KafkaModule] Producer (send/sendBatch)
│   ├── consumer-registry.service.ts # [ConsumerModule] Consumer lifecycle
│   ├── batch-processor.service.ts   # [ConsumerModule] Batch accumulation
│   ├── idempotency.service.ts       # [ConsumerModule] Duplicate prevention
│   ├── pressure-manager.service.ts  # [ConsumerModule] Back pressure
│   ├── dlq.service.ts               # [ConsumerModule] Dead Letter Queue
│   └── dlq-retry.service.ts         # [ConsumerModule] DLQ Retry
├── discovery/
│   └── consumer-discovery.service.ts # [ConsumerModule] Auto-discover @Consumer
└── health/
    └── kafka-health-indicator.ts    # [KafkaModule] Health checks
```

### Module Separation

- **KafkaModule**: Infrastructure (connections, producer, health check)
- **ConsumerModule**: Consumer logic (discovery, registry, batch, DLQ, etc.)

### Key Components

- **KafkaModule**: Root module with `forRoot()` and `forRootAsync()` for configuration
- **KafkaClient**: Producer service with `send()`, `sendBatch()`, `sendQueued()` methods
- **@Consumer() decorator**: Method decorator to define topic consumers with batch/pressure/DLQ options
- **ConsumerModule**: Auto-discovers and registers consumer methods on app startup
- **KafkaHealthIndicator**: Health checks for Kafka connections

### Features

- Intelligent batch processing with configurable size and timeout
- Key-based message grouping for ordered processing within batches
- Back pressure management (pause/resume consumption)
- In-memory idempotency with TTL
- Dead Letter Queue with exponential backoff retry
- Graceful shutdown

## Tech Stack

- NestJS 11
- kafkajs
- TypeScript (ES2023 target, CommonJS module)
- Jest for testing
- ESLint + Prettier for code style

## Usage Example

```typescript
// app.module.ts
@Module({
  imports: [
    // Infrastructure module (producer, connections)
    KafkaModule.forRoot({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    }),
    // Consumer module (required if using @Consumer decorator)
    ConsumerModule.forRoot(),
  ],
  providers: [OrderConsumer],
})
export class AppModule {}

// order.consumer.ts
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

// order.service.ts
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
