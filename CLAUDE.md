# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `@loipv/nestjs-kafka`, a production-ready NestJS module for Kafka client and consumer functionality built on top of confluent-kafka-javascript (librdkafka). The library provides batch processing, non-blocking retry (pause partition, redeliver) with a DLQ, idempotency, key-based grouping, multi-connection support and OpenTelemetry tracing.

## Commands

```bash
# Build
npm run build

# Testing
npm test               # Run unit tests (librdkafka is mocked; no native binary needed)
npm run test:watch     # Watch mode
npm run test:cov       # With coverage
npm run test:e2e       # Kafka e2e via testcontainers (Docker; needs `npm rebuild @confluentinc/kafka-javascript` after an --ignore-scripts install)
npm run test:smoke     # Install the packed tarball into test/smoke/* fixture apps and run them

# Run a single test file
npx jest lib/services/<file>.spec.ts

# Linting, formatting, types
npm run lint           # ESLint with auto-fix
npm run lint:check     # ESLint without --fix (CI)
npm run format         # Prettier
npm run typecheck      # tsc --noEmit

# Package and workflow checks
npm run check:pack       # Tarball contents, entry points, inlined sourcemaps
npm run check:pkg        # publint + attw
npm run check:workflows  # Security shape of .github/workflows (SHA pins, ci-ok gate, release rules)
npm run lint:workflows   # actionlint via Docker

# Releases
npm run changeset      # Add a changeset to every PR that changes the published package
```

Releases go through Changesets and the `Release` workflow (npm trusted publishing with provenance) — never `npm publish` by hand. See RELEASING.md.

Test files are colocated with source as `*.spec.ts` under `lib/` (jest `rootDir: lib`). The Kafka e2e suite lives in `test/kafka.e2e-spec.ts` (`test/jest-e2e.json`). This is a published npm library (`files: ["dist", "CHANGELOG.md"]`; `prepublishOnly` runs build, `check:pack`, `check:pkg`, typecheck and unit tests). Every unit spec that imports `@confluentinc/kafka-javascript` (directly or through a service) must `jest.mock` it: CI and the release job install with `--ignore-scripts`, so the native binding is absent.

## Architecture

```
lib/
├── index.ts                         # Main barrel export
├── kafka.module.ts                  # KafkaModule - forRoot/forRootAsync/forRootMultiple (per-connection providers)
├── kafka-core.module.ts             # KafkaCoreModule - static global module with the shared services
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
│   ├── kafka-core.service.ts        # [KafkaCoreModule] Connection management
│   ├── kafka-client.service.ts      # [KafkaCoreModule] Producer (send/sendBatch/sendMultiTopicBatch)
│   ├── tracing.service.ts           # [KafkaCoreModule] OpenTelemetry tracing
│   ├── consumer-registry.service.ts # [ConsumerModule] Consumer lifecycle and retry
│   ├── partition-retry-tracker.ts   # [ConsumerModule] Attempt counters and resume timers (plain class)
│   ├── batch-processor.service.ts   # [ConsumerModule] Batch splitting and key grouping
│   ├── idempotency.service.ts       # [ConsumerModule] Duplicate prevention
│   └── dlq.service.ts               # [ConsumerModule] Dead Letter Queue
├── discovery/
│   └── consumer-discovery.service.ts # [ConsumerModule] Auto-discover @Consumer
└── health/
    └── kafka-health-indicator.ts    # [KafkaCoreModule] Health checks
```

### Module Separation

- **KafkaModule**: Infrastructure (connections, producer, health check). Every `forRoot*()` call imports the static `KafkaCoreModule`, so all connections share one `KafkaCoreService`/`KafkaClient`; each call only adds its own options token, `KAFKA_INIT_<name>` registration provider and client token.
- **ConsumerModule**: Consumer logic (discovery, registry, batch, DLQ, etc.)

### Key Components

- **KafkaModule**: Root module with `forRoot()`, `forRootAsync()`, and `forRootMultiple()` for configuration
  - Supports multi-connection setup with named connections; `forRoot()`/`forRootAsync()` may be called several times
  - Producer options are passed to the client inside its `kafkaJS` block; numeric `compression` is mapped to a codec name
- **ConsumerModule**: Auto-discovers and registers consumer methods on app startup
  - Use `forRoot(options?)` in app module with optional default options
  - All `@Consumer()` decorated methods are automatically discovered via NestJS DiscoveryService
  - Default options are merged with `@Consumer` decorator options (decorator takes precedence)
- **KafkaClient**: Producer service with `send()`, `sendBatch()`, `sendMultiTopicBatch()`; pass `{ connection }` to target a named connection
  - `ConnectionBoundClient`: what `@InjectKafkaClient(name)` injects for a non-default connection
- **@Consumer() decorator**: Method decorator to define topic consumers with batch/retry/DLQ options
  - Default `groupId`: `${topic}-group` if not specified
  - Multiple topics with the same `groupId` share one consumer group (multi-topic consumer); they must all be batch or all non-batch (mixing throws at registration)
  - `disabled: true`: Skip this consumer at startup without removing the decorator
  - `deserialize: false`: Receive raw `KafkaMessage` without JSON/string deserialization (default: true)
- **@InjectKafkaClient() decorator**: Inject named connection clients in services
- **KafkaHealthIndicator**: `isHealthy` = every producer connected and every consumer group running (status kept in `KafkaCoreService`); `checkBrokers` / `checkConsumerLag` probe with an admin client
- **TLS**: `ssl` object mapped to librdkafka `ssl.*` keys (unknown keys throw); `rdkafka` passes raw librdkafka properties, applied last

### Features

- Multi-connection support (connect to multiple Kafka clusters simultaneously)
- Batch processing: fetched messages split into handler calls of at most `batchSize`; the consumer's `js.consumer.max.batch.size` is set to the group's largest `batchSize` (default 100)
- Key-based message grouping for ordered processing within batches
- In-memory idempotency with TTL (requires an `idempotencyKey` extractor), keys namespaced `<connection>:<groupId>|<topic>`, capped by `idempotencyMaxKeys`
- Retry by pausing the partition with capped exponential backoff, then an optional Dead Letter Queue (optionally on another connection)
- Consumers start in `ConsumerModule.onApplicationBootstrap`; producers connect at `KafkaCoreService.onApplicationBootstrap` (not awaited)
- **OpenTelemetry tracing** (distributed tracing across produce → consume)
- Graceful shutdown with proper cleanup

## Important Behavior Notes

### Retry Mechanism — Pause Partition and Rethrow

**Never sleep inside a handler.** Everything that can throw in `eachMessage` / a chunk (idempotency extractor included) runs inside the try, and `handleHandlerFailure` turns a non-Error rejection into an `Error` (the client's catch block would otherwise throw on `${e}` and skip its seek-back, losing the message). Handler `retry` options are NOT copied into the client's `kafkaJS.retry`. On a handler failure `ConsumerRegistryService.handleHandlerFailure` asks for a verdict (`evaluateRetry`, or `DlqService.handleFailure` when a DLQ is configured):
- `retry`: pause the payload's partition (`payload.pause()`), schedule its resume through `PartitionRetryTracker.scheduleResume` after `delayMs`, and **rethrow**. The client seeks back and redelivers after the resume. The consumer keeps polling, so other partitions keep flowing and the member is never evicted.
- `complete` (dead-lettered or skipped): return, so the offset is stored.

Delays are `initialRetryTime × multiplier^(n-1)`, capped at `retry.maxRetryTime` (default `MAX_RETRY_DELAY_MS` = 30s); the DLQ backoff uses the same cap. **Handler errors NEVER crash the consumer.**

1. **Default** (`skipMessageOnMaxRetries: false`): the message is retried **indefinitely** — never dropped, never lost. `retry.retries` is only a milestone for the skip decision.
2. **Skip mode** (`skipMessageOnMaxRetries: true`): message is skipped after `retries` attempts and the offset commits.

Attempt counters are in memory, keyed `<connection>:<groupId>|<topic>|<partition>|<offset>` (a chunk uses its first offset). They are cleared on success, on `complete`, on revoke and on shutdown; a restart or a moved partition restarts the ladder.

**Revoke:** every group consumer gets a top-level `rebalance_cb`. On `ERR__REVOKE_PARTITIONS` (-174) it drops retry state for the revoked partitions and calls `consumer.resume()` for them while they are still assigned — the client keeps its own paused set across rebalances, so a re-assigned partition would otherwise stay paused.

**Infra failures:** `consumer.run()` in confluent-kafka-javascript returns as soon as the fetch loop starts and never rejects for handler or broker errors: the client logs them, seeks back and redelivers the message, and keeps reconnecting on its own. There is no library-level restart. The registry awaits `run()`, so an error while starting a consumer (connect, subscribe, run) fails application startup, and the consumer is disconnected first. `DlqService.handleFailure` never rejects: a failed DLQ send, or one exceeding `dlq.sendTimeout` (default 30s), is a `retry` verdict.

**Batch consumers** (`eachBatchAutoResolve = false`) resolve the offset after each successful chunk. A failing chunk pauses and rethrows; the client seeks to the first unresolved offset, so earlier chunks are not reprocessed. An all-duplicate chunk is resolved without calling the handler. Batch handlers must be idempotent (at-least-once).

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
  // If this keeps failing, it is redelivered after 1s, 2s, 4s ... capped at 30s
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

When **using DLQ**, failed messages are sent to the DLQ topic after `dlq.maxRetries` (batch consumers too: one `sendBatch` per exhausted chunk, idempotency-filtered messages excluded). Values and keys are sent unchanged (Buffers byte-exact). The message is NOT skipped or dropped: if the DLQ send fails or times out, it is retried with the same backoff. Reprocessing the DLQ is the application's job (an ordinary `@Consumer` on the DLQ topic).

### Graceful Shutdown

`ConsumerRegistryService.gracefulShutdown()` is idempotent (it memoises its promise) and runs only from the registry's own `onApplicationShutdown`. It clears every resume timer and counter, then disconnects the consumers. During shutdown `eachMessage` **throws** instead of returning: a normal return stores the offset, which the client commits on close, losing the message. `eachBatch` may return, because nothing unresolved is stored. `KafkaCoreService.disconnectAll()` waits up to 5s for an in-flight producer connect (the one started at bootstrap), then disconnects in every case. Nest skips shutdown hooks when initialisation fails, so `ConsumerModule` calls `ConsumerRegistryService.abortStartup()` (disconnect started groups and every producer) before rethrowing a register/start error; `startAll()` waits for every group to settle before rejecting.

Any new retry or delay logic must go through `PartitionRetryTracker` (unref'd timers, cleared on shutdown), never a sleep inside a handler.

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
