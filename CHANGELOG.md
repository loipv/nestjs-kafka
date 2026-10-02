# Changelog

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
