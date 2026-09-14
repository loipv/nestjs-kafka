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
npx jest lib/services/<file>.spec.ts

# Linting & Formatting
npm run lint           # ESLint with auto-fix
npm run format         # Prettier

# Publish to npm
npm publish --access public
```

Test files are colocated with source as `*.spec.ts` under `lib/` (jest `rootDir: lib`). `test/jest-e2e.json` exists for future e2e tests but none are written yet. This is a published npm library (`files: ["dist"]`, `prepublishOnly` builds).

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
  - `sendQueued()`: Buffers messages and auto-flushes at 100 messages or 100ms timeout
  - `forConnection(name)`: Returns a `ConnectionBoundClient` for fluent named-connection usage
- **@Consumer() decorator**: Method decorator to define topic consumers with batch/pressure/DLQ options
  - Default `groupId`: `${topic}-group` if not specified
  - Multiple topics with the same `groupId` share one consumer group (multi-topic consumer)
  - `disabled: true`: Skip this consumer at startup without removing the decorator
  - `deserialize: false`: Receive raw `KafkaMessage` without JSON/string deserialization (default: true)
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

### Retry Mechanism Without DLQ — Split-Error Policy

Handler failures are retried **in-process** with capped exponential backoff (`initialRetryTime × multiplier^n`, capped at `maxRetryTime`, default 30s). **Handler errors NEVER crash the consumer.**

1. **Default** (`skipMessageOnMaxRetries: false`): the message is retried **indefinitely** — never dropped, never lost. `retry.retries` is only a milestone for the skip/DLQ decision, NOT a hard limit in this mode.
2. **Skip mode** (`skipMessageOnMaxRetries: true`): message is skipped after `retries` attempts and the offset commits.

**Auto-restart (infra crashes only):** only infra failures (Kafka connection loss, fatal client errors, DLQ send failure) kill the consumer run loop (surfaced via `consumer.run()` rejection). `scheduleConsumerRestart` rebuilds the consumer (new instance, same groupId) with the same capped exponential backoff, **unlimited attempts** — when the Kafka connection is healthy again, the app recovers on its own.

**Known limitation:** indefinite in-process retry can cumulatively exceed librdkafka's `max.poll.interval.ms` (default 5 min) → the consumer is evicted and rebalances periodically while a poison message persists (no data loss; the crash is absorbed by auto-restart). For retries spanning minutes, configure a DLQ.

**Batch consumers** share the same policy and resolve offsets only after a successful flush (at-least-once) — batch handlers must be idempotent.

**Example:**
```typescript
// Default behavior: retry indefinitely, consumer never crashes
@Consumer('orders', {
  retry: {
    retries: 3,
    initialRetryTime: 1000,
    multiplier: 2,
    // skipMessageOnMaxRetries: false (default)
  },
})
async handleOrder(message: KafkaMessage) {
  // If this keeps failing, it is re-invoked: 1s, 2s, 4s ... capped at 30s
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

### Graceful Shutdown & In-Flight Delays

`consumer-registry.service.ts` (the single retry/restart `sleep()`) and `dlq-retry.service.ts` (DLQ reprocess delays) use a `shutdownResolvers: Set<() => void>` pattern with a cancellable `sleep()` helper. On shutdown, all pending delays are immediately resolved so the process does not block waiting for delays that can be up to several minutes; `sleep()` also resolves immediately if shutdown was signalled before it registered. A retry aborted by shutdown rethrows **once** so the offset is never committed (message redelivered next boot). Any new code that adds sleep-based retry loops must follow this same pattern.

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

See README.md for full usage docs (module setup, batch/DLQ/idempotency examples, multi-connection). Key shape: `KafkaModule.forRoot()` in the root module for infrastructure, `ConsumerModule.forRoot()` for consumer defaults, and `@Consumer()` methods in feature-module providers are auto-discovered — feature modules need no Kafka imports.
