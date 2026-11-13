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
npm run test:cov       # With coverage; fails below jest.coverageThreshold (measured % rounded down, minus 1 point of headroom; raise it when coverage rises)
npm run test:e2e       # Kafka e2e via testcontainers (Docker; needs `npm rebuild @confluentinc/kafka-javascript` after an --ignore-scripts install)
npm run test:smoke     # Install the packed tarball into test/smoke/* fixture apps and run them

# Run a single test file
npx jest lib/services/<file>.spec.ts

# Linting, formatting, types
npm run check:specs    # Every lib/**/x.spec.ts has a sibling x.ts (one spec per source file)
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

Test files are colocated with source as `*.spec.ts` under `lib/` (jest `rootDir: lib`). The Kafka e2e suite lives in `test/kafka.e2e-spec.ts` (`test/jest-e2e.json`). This is a published npm library (`files: ["dist", "CHANGELOG.md"]`; `prepublishOnly` runs build, `check:pack`, `check:pkg`, typecheck and unit tests). Every unit spec that imports `@confluentinc/kafka-javascript` (directly or through a service) gets the shared manual mock `lib/__mocks__/@confluentinc/kafka-javascript.ts` (Jest applies a node-module mock under a root's `__mocks__` automatically) and still calls `jest.mock('@confluentinc/kafka-javascript')` without a factory, so the dependency on the mock stays visible: CI and the release job install with `--ignore-scripts`, so the native binding is absent.

Test layout (enforced in part by `npm run check:specs`):

- Each source file `lib/**/x.ts` has at most one spec, `lib/**/x.spec.ts`, next to it. All tests for `x.ts` live there, grouped with `describe`; no per-feature spec files (`x.retry.spec.ts`).
- A spec is named after the source file it tests: `@InjectKafkaClient` tests go in `inject-kafka-client.decorator.spec.ts`, not in a module spec.
- Shared test code lives only in `lib/__mocks__/`.
- The Kafka e2e suite stays one file, `test/kafka.e2e-spec.ts`; smoke fixtures live in `test/smoke/<fixture>`.
- No spec is required for barrels (`index.ts`), `interfaces/*` without logic, `decorators/constants.ts` and `kafka-core.module.ts`.
- Test names describe behaviour in the present tense without a `should` prefix (`returns down when lag exceeds the threshold`).

## Architecture

```
lib/
├── index.ts                         # Main barrel export
├── kafka.module.ts                  # KafkaModule - forRoot/forRootAsync, one call per connection (per-connection providers)
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
├── health/
│   └── kafka-health-indicator.ts    # [KafkaCoreModule] Health checks (plain HealthIndicatorResult objects)
└── __mocks__/@confluentinc/kafka-javascript.ts # Shared Kafka client mock for unit specs (not built)
scripts/check-specs.mjs              # One spec per source file (npm run check:specs)
```

### Module Separation

- **KafkaModule**: Infrastructure (connections, producer, health check). Every `forRoot*()` call imports the static `KafkaCoreModule`, so all connections share one `KafkaCoreService`/`KafkaClient`; each call only adds its own options token, `KAFKA_INIT_<name>` registration provider and client token.
- **ConsumerModule**: Consumer logic (discovery, registry, batch, DLQ, etc.)

### Key Components

- **KafkaModule**: Root module with `forRoot()` and `forRootAsync()`, called once per connection
  - Multi-connection: one `forRoot()`/`forRootAsync()` call per named connection; the same name registered again with different options fails at startup (identical options are accepted)
  - Producer options are passed to the client inside its `kafkaJS` block; numeric `compression` is mapped to a codec name
- **ConsumerModule**: Auto-discovers and registers consumer methods on app startup (`autoStart: false` defers starting to `ConsumerRegistryService.startAll()`)
  - Discovery: static (singleton) providers only, each instance once (a `useExisting` alias is the same instance), `@Consumer` methods along the prototype chain (nearest name wins), descriptor-only reads; request-scoped and transient providers with `@Consumer` are skipped with a warning (a transient wrapper's instance never ran its constructor)
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
- **KafkaHealthIndicator**: returns plain `HealthIndicatorResult` objects (Terminus 11 treats `down` as a failed check); `isHealthy` = every producer connected and every consumer group running (status kept in `KafkaCoreService`); `checkBrokers` / `checkConsumerLag` probe with the connection's shared admin (`KafkaCoreService.getAdmin(name?)`, connected once, closed by `disconnectAll`), take an optional connection name, and list `unreadableTopics` in lag details
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

**Revoke:** every group consumer gets a top-level `rebalance_cb`. On `ERR__REVOKE_PARTITIONS` (-174) it drops retry state for the revoked partitions and calls `consumer.resume()` for them while they are still assigned — the client keeps paused partitions (its own `#pausedPartitions` map, mirrored by a librdkafka pause) across rebalances, so a re-assigned partition would otherwise stay paused.

**Infra failures:** `consumer.run()` in confluent-kafka-javascript returns as soon as the fetch loop starts and never rejects for handler or broker errors: the client logs them, seeks back and redelivers the message, and keeps reconnecting on its own. There is no library-level restart. The registry awaits `run()`, so an error while starting a consumer (connect, subscribe, run) fails application startup, and the consumer is disconnected first. `DlqService.handleFailure` never rejects: a failed DLQ send, or one exceeding `dlq.sendTimeout` (default 30s), is a `retry` verdict.

**Commits:** with `autoCommit: false` the client never commits by itself, so the registry commits synchronously: `consumer.commitOffsets([{ topic, partition, offset: offset + 1 }])` after every normal return of `eachMessage` (handled, duplicate, no handler, skipped, dead-lettered) and `payload.commitOffsetsIfNecessary()` after each resolved chunk. A failed commit is logged, never rethrown. Timer options (`retry.initialRetryTime`, `retry.maxRetryTime`, `dlq.sendTimeout`, `dlq.retryDelay`) must be positive and at most 2147483647 ms; `retry.multiplier` and `dlq.retryBackoffMultiplier` must be finite and >= 1. A failed commit is only logged: the partition's next commit covers it, otherwise the message is redelivered after a restart. Each synchronous commit waits at most `COMMIT_TIMEOUT_MS` (10s); a later settle is ignored. No revoke guard is needed: the client queues a revoke's unassign until its workers settle, so an in-flight delivery's commit always lands before the partition moves.

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

When **using DLQ**, failed messages are sent to the DLQ topic after `dlq.maxRetries` (batch consumers too: the exhausted chunk minus idempotency-filtered messages). `DlqService.sendToDlq` sends one message per `kafkaClient.send` in parallel and records each success in the delivery's `PartitionRetryTracker.deadLettered(attemptKey)` set, so a retry after a partial failure only sends what did not arrive. Values and keys are sent unchanged (Buffers byte-exact). The message is NOT skipped or dropped: if the DLQ send fails or times out, it is retried with the same backoff. Reprocessing the DLQ is the application's job (an ordinary `@Consumer` on the DLQ topic).

### Graceful Shutdown

`ConsumerRegistryService.gracefulShutdown()` is idempotent (it memoises its promise) and runs only from the registry's own `onModuleDestroy`. Nest runs every `onModuleDestroy` before any `onApplicationShutdown`, so consumers always stop before `KafkaClient.onApplicationShutdown` disconnects producers, whatever the import order. Both library modules are global: Nest bootstraps them first and destroys them last (an application's own `onModuleDestroy` runs while consumers still run). It clears every resume timer and counter, then disconnects the consumers. During shutdown `eachMessage` **throws** instead of returning: a normal return stores the offset, which the client commits on close, losing the message. `eachBatch` may return, because nothing unresolved is stored. `KafkaCoreService.disconnectAll()` waits up to 5s for an in-flight producer connect (the one started at bootstrap), then disconnects in every case. Nest skips shutdown hooks when initialisation fails, so `ConsumerModule` calls `ConsumerRegistryService.abortStartup()` (disconnect started groups and every producer) before rethrowing a registration error, and `startAll()` itself waits for every group to settle, runs `abortStartup()` (a cleanup failure is only logged) and rethrows the start error. `startAll()` is memoised (concurrent and repeated calls share one start), rejects before `registerConsumers()` ran (ConsumerModule always calls it at bootstrap, even with no consumers) and once shutdown began; a group start racing a shutdown stops at its next step, and `shutdown()` disconnects every group that has a consumer. `ConsumerModule.forRoot({ autoStart: false })` registers at bootstrap but leaves `startAll()` to the application (`ConsumerRegistryService` is exported for that).

Any new retry or delay logic must go through `PartitionRetryTracker` (unref'd timers, cleared on shutdown), never a sleep inside a handler.

### OpenTelemetry Tracing

The library supports distributed tracing with OpenTelemetry. When enabled, trace context is:
1. **Injected** into Kafka message headers when producing, through the application's registered OpenTelemetry propagator (W3C Trace Context by default; B3 and baggage work too)
2. **Extracted** from headers when consuming (string, Buffer or array values), linking producer and consumer spans

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
5. **Platform support**: Linux (x64/arm64), macOS (arm64), Windows (x64), Node.js 20, 22, 24. Development needs Node ≥ 22.11 (devDependencies; `.nvmrc` pins 24)

## Usage Example

See README.md for full usage docs (module setup, batch/DLQ/idempotency examples, multi-connection). Key shape: `KafkaModule.forRoot()` in the root module for infrastructure, `ConsumerModule.forRoot()` for consumer defaults, and `@Consumer()` methods in feature-module providers are auto-discovered — feature modules need no Kafka imports.
