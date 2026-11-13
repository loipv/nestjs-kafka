# Changelog

## 2.0.0

### Major Changes

- [`98c539d`](https://github.com/loipv/nestjs-kafka/commit/98c539d052f7a9e9750bcebcadd9b834790b9f87) Thanks [@loipv](https://github.com/loipv)! - Make `autoCommit: false` commit, remove `KafkaModule.forRootMultiple()`, bound timer options, trace through the application's OpenTelemetry propagator, and ship JSDoc in the published types. See "Changed behaviour" and "Removed" in the README's "Migration Guide (from v1.x to v2.x)".

  Fixes:
  - `autoCommit: false` commits each handled message (or batch chunk) synchronously; offsets used to be stored but never committed, so the group re-read everything after a restart.
  - `retry.initialRetryTime`, `retry.maxRetryTime` and `dlq.sendTimeout` above 2147483647 ms fail at startup instead of firing after 1 ms (a DLQ send then always timed out and the message was retried forever).
  - A `ConsumerModule.forRoot()` default (including each `retry` field) is no longer hidden by an explicit `undefined` in `@Consumer()` options.
  - Consumer discovery no longer reads provider getters, which could trigger their side effects.
  - Producers of several connections disconnect in parallel on shutdown.
  - The published type declarations keep their JSDoc.

  Breaking changes:
  - `KafkaModule.forRootMultiple()` is removed: call `KafkaModule.forRoot()` once per connection.
  - `rdkafka: { 'enable.auto.commit': ... }` fails at startup: use the consumer `autoCommit` option.
  - A second registration of a connection name with different options fails at startup (it used to be ignored with a warning).
  - Timer options must be positive (`retry.initialRetryTime: 0` now fails at startup); `dlq.retryDelay` is bounded like the other delays, and `retry.multiplier` / `dlq.retryBackoffMultiplier` must be finite and >= 1.
  - A conflicting connection registration fails with `Kafka connection "<name>" is already registered with different options (<keys>)`.
  - Trace context goes through the registered OpenTelemetry propagator; without one, no trace headers are written.
  - `KafkaHealthIndicator` no longer has an optional `HealthIndicatorService` constructor parameter (it was never injected); it returns plain results, which Terminus evaluates as before.

- [`ebb2919`](https://github.com/loipv/nestjs-kafka/commit/ebb2919e8c971de8d8335283d8da84dc85c1244d) Thanks [@loipv](https://github.com/loipv)! - Retry failed messages without blocking the consumer, and fix message loss, data corruption, batch sizing, startup order, health and TLS. See "Changed behaviour" in the README's "Migration Guide (from v1.x to v2.x)".

  Fixes:
  - A message being processed during shutdown is no longer acknowledged; it is redelivered on the next start instead of being lost.
  - DLQ messages keep the original value and key bytes; binary payloads (Avro, Protobuf) were corrupted by a UTF-8 conversion. Producer keys `0` and `''` are no longer sent as `null`.
  - `ssl: { ca, cert, key, passphrase, rejectUnauthorized }` is applied as librdkafka properties; the object used to be reduced to `ssl: true`.
  - `batchSize` above 32 takes effect: the client fetches up to the group's largest `batchSize` per call.
  - Two `@Consumer`s for the same topic in one group fail at startup instead of one silently replacing the other.
  - A consumer whose start fails after connecting is disconnected; when consumer startup fails, the consumers that did start and the producers are closed too, so a failed boot no longer keeps the process alive.
  - Consumers start in `onApplicationBootstrap`, after every provider's `onModuleInit`.
  - Idempotency keys are scoped per connection, group and topic, batch consumers honour `idempotencyTtl`, and the store is capped by `ConsumerModule.forRoot({ idempotencyMaxKeys })` (default 100000).

  Breaking changes:
  - A failing handler is no longer re-run inside the same call. The library pauses the message's partition, rethrows, and resumes after the backoff delay; the client redelivers the message. Other partitions keep flowing and long retries no longer get the member evicted. Each retry logs an extra line from the Kafka client.
  - `retry.maxRetryTime` (default 30000) caps every retry delay, DLQ backoff included. Consumer `retry` options no longer change the client's request backoff.
  - A throwing `idempotencyKey` extractor is handled like a handler failure instead of redelivering in a tight loop. `idempotencyMaxKeys` must be a positive integer and `dlq.sendTimeout` a positive number.
  - A failed DLQ send is retried with the backoff delay; new `dlq.sendTimeout` (default 30000) bounds each send.
  - Batch consumers dead-letter after `dlq.maxRetries` (was `retry.retries`), in one send per failing chunk, and commit after each successful chunk.
  - `KafkaHealthIndicator.isHealthy` is up only when every producer is connected and every consumer group is running; its details are `{ producers, consumers }` instead of `connected`. Producers connect at startup, and a disconnected producer is reconnected in the background when `isHealthy` runs.
  - An `ssl` object with an unknown key fails at startup. New `rdkafka` option passes raw librdkafka properties.
  - `DlqService.handleFailure()` and `sendToDlq()` take `messages: KafkaMessage[]`; `handleFailure()` never rejects. `IdempotencyService` methods take a `namespace` argument. `KafkaHealthIndicator` no longer depends on `KafkaClient`.

- [`bf627b9`](https://github.com/loipv/nestjs-kafka/commit/bf627b937d3fb6693eb62f059a4f66f81449538c) Thanks [@loipv](https://github.com/loipv)! - Fix a startup failure with aliased consumer providers, make shutdown and startup order deterministic, and close the remaining DLQ, commit and health edge cases. See "Fixed", "Changed behaviour" and "Removed" in the README's "Migration Guide (from v1.x to v2.x)".

  Fixes:
  - A consumer provider also exposed through `useExisting` no longer fails startup with "already has a handler for topic".
  - `@Consumer` methods declared on a base class are discovered (registering both a base class and its subclass as providers now fails at startup: they handle the same topic); request-scoped, request-scoped-dependent and transient consumer providers are skipped with a warning naming the reason instead of registering a handler without its dependencies.
  - A failed DLQ chunk logs one aggregate error instead of one error per message.
  - Consumers always stop before producers disconnect, whatever the import order of `KafkaModule` and `ConsumerModule`.
  - A partially failed DLQ send only re-sends the messages that did not arrive.
  - A synchronous commit (`autoCommit: false`) waits at most 10s.
  - `checkConsumerLag` lists topics whose offsets cannot be read in `unreadableTopics` instead of counting them as lag 0.

  Features:
  - `ConsumerModule.forRoot({ autoStart: false })` with `ConsumerRegistryService.startAll()` (now exported) to start consumers after the application is ready.
  - `checkBrokers` / `checkConsumerLag` accept a connection name and reuse one admin client per connection.

  Breaking changes:
  - `KAFKA_MODULE_OPTIONS` is removed: inject `getKafkaOptionsToken(name)` (`getKafkaOptionsToken()` for the default connection).
  - `ConsumerRegistryService` stops consumers in `onModuleDestroy` (was `onApplicationShutdown`).
  - `ConsumerRegistryService.startAll()` is memoised, closes started consumers and producers before rejecting, rejects before the application is initialised and once shutdown has begun (a start racing a shutdown stops instead of leaving a consumer running).
  - DLQ sends go out one message per send; `DlqService.handleFailure()` / `sendToDlq()` take an optional `sent` set.

- [`a804b28`](https://github.com/loipv/nestjs-kafka/commit/a804b287801a575bacb922a6e3bcd1046491b2f2) Thanks [@loipv](https://github.com/loipv)! - Remove features that did not work as documented, and fix two configurations that failed at startup. See "Migration Guide (from v1.x to v2.x)" in the README.

  Fixes:
  - `KafkaModule.forRoot({ producer: { ... } })` no longer crashes at startup: producer options now reach the client inside its `kafkaJS` block. `compression` accepts a codec name (`'gzip'`) as well as the old number (`1`); an unknown value fails at registration.
  - Producer options left `undefined` (for example an unset environment variable) are ignored instead of breaking every send.
  - A producer whose first connect fails is rebuilt, so a later send (such as a redelivered DLQ send) reconnects once the broker is back.
  - Several `KafkaModule.forRoot()` / `forRootAsync()` calls now share one connection registry, so consumers and sends can use a connection registered by any of them. Tracing is enabled by the first connection that sets `tracing.enabled`.

  Breaking removals:
  - DLQ auto-retry (`dlq.retry`, `finalDlqTopic`, `DlqRetryOptions`, `DlqRetryService`). Reprocess a DLQ with an ordinary `@Consumer`.
  - The DLQ circuit breaker (`CircuitBreakerService`, `DlqService.getCircuitState()` / `resetCircuit()`). A failed DLQ send is now always redelivered instead of being dropped once the circuit opened.
  - DLQ metrics (`DlqMetricsService`).
  - `KafkaClient.sendQueued()` and `KafkaClient.forConnection()`; `SendOptionsWithConnection` (use `SendOptions`, now `{ connection?: string }`). `ConnectionBoundClient` methods no longer take an options argument.
  - Back pressure (`backPressureThreshold`, `maxQueueSize`, `maxConcurrency`, `PressureManagerService`, the `PressureState` / `PressureManagerOptions` types), which had no effect.
  - `batchTimeout` and the batch accumulator: a batch handler receives the fetched messages split into chunks of `batchSize`.
  - Consumer groups that mix batch and non-batch consumers now fail at startup.
  - Consumer auto-restart. The client never stops on handler or broker errors; an error from `consumer.run()` at startup now fails application startup.
  - No-op or unused API: `ConsumerModule.clearConsumers()`, `DlqService.clearRetryState()`, `ConsumerDiscoveryService.registerHandler()` / `clearConsumers()`, `getKafkaCoreToken()`, `KafkaModuleOptions.defaultConsumerGroupId`, function-valued `brokers`, `ConsumerOptions.topic`.
  - `TracingService` takes no constructor arguments; tracing is configured per connection through `configure()`.

## 1.3.0

### Minor Changes

- [`cfdf91e`](https://github.com/loipv/nestjs-kafka/commit/cfdf91e4a042aa1357d59a1ee98bed47d741bbfb) Thanks [@loipv](https://github.com/loipv)! - Declare Node.js `>=20` in `engines`. The package now ships `LICENSE` and `CHANGELOG.md`, its sourcemaps embed their sources, and releases are published from GitHub Actions with npm provenance.

## 1.2.1 (2026-10-02)

### Features

- **feat:** `dlq.connection` — send the DLQ (and run DLQ auto-retry / `finalDlqTopic`) on a different named connection than the consumer. Unknown connection names fail at startup

### Tests

- **test:** e2e for `dlq.connection` against a second Kafka container (DLQ, auto-retry and final DLQ land only on the DLQ cluster; unknown connection fails startup)

### Fixes

- **fix:** idempotency cleanup timer is `unref()`'d — a failed app init no longer leaves the process hanging
- **fix:** `allowAutoTopicCreation` now actually creates the DLQ / `finalDlqTopic` topics (previously a lookup error silently aborted creation after the main topic)

## 1.2.0 (2026-09-14)

### Fixes

- **fix:** `KafkaClient.send*` no longer double-encodes `Buffer` payloads as JSON
- **fix:** message retry now re-invokes the handler in-process with capped exponential backoff (previously: sleep-then-throw, which stopped the consumer without redelivering). With `skipMessageOnMaxRetries: false` (default) the message is retried **indefinitely** — handler errors never crash the consumer and never lose the message
- **fix:** batch consumers resolve offsets only after successful processing (at-least-once). Batch handlers now get retry + DLQ support
- **fix:** `isHealthy()` reports actual producer connection state (was: registration existence). Use `checkBrokers()` for an active connectivity probe
- **fix:** producer disconnect always runs on shutdown, even if the final queued-batch flush fails
- **fix:** idempotency store sweeps all expired entries per cycle (removes memory growth under sustained load)
- **fix:** module-default `retry` options field-merge with decorator options

### Features

- **feat:** consumer run-loop crashes (Kafka connection/fatal infra errors, including DLQ send failures) auto-restart with exponential backoff capped at `retry.maxRetryTime` — unlimited attempts, so a healthy Kafka connection means a running app
- **feat:** `autoCreateTopicPartitions` / `autoCreateTopicReplicationFactor` options for auto-created topics (default 1/1, previously hardcoded)
- **chore:** upgrade `@confluentinc/kafka-javascript` to `^1.10.1` (librdkafka 2.15.1)

### Tests

- **test:** full unit coverage for core services (registry, DLQ, DLQ-retry, client, idempotency, batch, circuit breaker, tracing, core, discovery)
- **test:** Kafka testcontainers e2e suite — `npm run test:e2e` (requires Docker): round-trip, in-process retry, DLQ round-trip, batch delivery, idempotency dedupe, graceful shutdown

### Deprecated

- `ConsumerModule.clearConsumers()` (no-op), `DlqService.clearRetryState()` (no-op)
