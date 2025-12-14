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
├── kafka.module.ts                  # KafkaModule with forRoot/forRootAsync
├── consumer.module.ts               # ConsumerModule lifecycle
├── interfaces/
│   ├── kafka-module-options.interface.ts
│   ├── consumer-options.interface.ts
│   └── message.interface.ts
├── decorators/
│   ├── consumer.decorator.ts        # @Consumer() method decorator
│   └── constants.ts
├── services/
│   ├── kafka-core.service.ts        # Kafka connection management
│   ├── kafka-client.service.ts      # Producer (send/sendBatch)
│   ├── consumer-registry.service.ts # Consumer lifecycle
│   ├── batch-processor.service.ts   # Batch accumulation
│   ├── idempotency.service.ts       # Duplicate prevention
│   ├── pressure-manager.service.ts  # Back pressure
│   └── dlq.service.ts               # Dead Letter Queue
├── discovery/
│   └── consumer-discovery.service.ts # Auto-discover @Consumer methods
└── health/
    └── kafka-health-indicator.ts    # Health checks
```

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
    KafkaModule.forRoot({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    }),
    ConsumerModule,
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
