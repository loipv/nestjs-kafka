# Performance audit (pre-2.0.0)

## Scope

- Date: 2026-11-14. Baseline commit: `3ed7c28`.
- Node v24.22.2, Apple M4 (macOS).
- Static read of `lib/` (consume, commit, retry, idempotency, DLQ, producer, tracing, health), then
  measurements of the consume path in memory (`npm run bench`) and against a local Kafka broker
  (`npm run bench:kafka`).
- Rule for fixes: no observable behaviour change and no public signature change.

## Findings

| # | Finding | Where | Status |
|---|---------|-------|--------|
| 1 | `autoCommit: false` commits synchronously after every message (one broker round trip per message, plus a timer and a `Promise.race`) | `consumer-registry.service.ts` (`commitMessageOffset`, `boundedCommit`) | Not fixed: changing the commit model changes delivery semantics. Measured below and documented in the README. |
| 2 | `deserializeMessageValue` runs `JSON.parse` in a `try`/`catch` for every value; every non-JSON value throws | `interfaces/message.interface.ts` | _pending_ |
| 3 | Idempotency calls the key extractor and builds the namespaced key twice per message (check, then mark); batch chunks do the same per message | `idempotency.service.ts`, `consumer-registry.service.ts` | _pending_ |
| 4 | `checkConsumerLag` fetches the latest offsets one topic at a time and looks up partitions with `Array.find` | `health/kafka-health-indicator.ts` | _pending_ |
| 5 | `PartitionRetryTracker.revoke` copies and scans every counter and DLQ set once per revoked partition | `services/partition-retry-tracker.ts` | _pending_ |
| 6 | `maxBytesPerPartition` is hardcoded at 1 MB | `consumer-registry.service.ts` (`buildConsumerConfig`) | Not fixed: a new option is out of scope. Raw librdkafka properties can be passed through `rdkafka`. |
| 7 | `partitionsConsumedConcurrently` defaults to 1 (the client's default) | client default | Not fixed: a default, not a defect. The README points to it for `autoCommit: false`. |
| 8 | The idempotency cleanup sweeps the whole key map synchronously every 60 s (up to `idempotencyMaxKeys`, default 100000) | `idempotency.service.ts` | Not fixed: a sweep of 100k entries takes a few ms once a minute. |
| 9 | Batch tracing extracts the propagation context of every message of a chunk (one span parent, the rest links) | `tracing.service.ts` | Not fixed: needed for the span links; only runs with tracing enabled. |
| 10 | `logger.debug` builds its message string on every send even when debug is off | `kafka-client.service.ts`, `consumer-registry.service.ts` | Not fixed: Nest's `Logger.isLevelEnabled('debug')` returns `false` when no log levels are configured (the default `ConsoleLogger` setup and custom loggers such as pino), so a guard would hide debug logs that apps see today. The cost is one template string per send. |

## Consume micro-benchmark

`npm run bench`: a real `ConsumerRegistryService` driven through the run config captured from a
fake consumer (no broker). No-op handler, 10k messages of warm-up, then the median of 5 runs of
100k messages. Batch scenarios fetch 500 messages per `eachBatch` call, `batchSize: 100`.
`+idem` sets an `idempotencyKey` extractor (unique keys, so nothing is deduplicated). JSON values
are `{"id":<n>,"status":"ok"}`, text values `order <n> processed`.

| Scenario | Before (msg/s) | After (msg/s) |
|----------|---------------:|--------------:|
| message/json | 424,124 | |
| message/text | 157,334 | |
| message/json+idem | 266,450 | |
| batch/json | 591,637 | |
| batch/json+idem | 335,770 | |

A second baseline run was within 3% of these numbers.

## autoCommit: false on Kafka

`npm run bench:kafka`: one `confluentinc/cp-kafka:7.6.0` broker (KRaft, single node) in Colima
(2 CPUs, 2 GiB) on the same machine. 20,000 messages (`{"id":<n>}`, 64 distinct keys) are produced
to each topic first. Each scenario is a Nest app with one `@Consumer` (no-op handler, fresh
`groupId`, `fromBeginning: true`). Throughput is measured from the first message received to the
20,000th, so the group join is excluded.

| Scenario | Topic | Run 1 (msg/s) | Run 2 (msg/s) |
|----------|-------|--------------:|--------------:|
| `autoCommit: true` (default) | 1 partition | 152,241 | 145,832 |
| `autoCommit: false` | 1 partition | 21,130 | 21,941 |
| `autoCommit: false`, `partitionsConsumedConcurrently: 4` | 4 partitions | 43,994 | 43,535 |

On a local broker `autoCommit: false` is about 7× slower than the default: each message waits for
its own commit round trip. Commits of different partitions overlap, so
`partitionsConsumedConcurrently` with more partitions roughly doubles it here (4 partitions on a
2-CPU broker). Against a remote broker the round trip is longer, so the gap grows. A coalesced
commit model would remove most of this cost; it changes the redelivery window after a crash and is
left for a separate feature.
