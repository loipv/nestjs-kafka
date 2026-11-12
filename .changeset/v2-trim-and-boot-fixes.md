---
'@loipv/nestjs-kafka': major
---

Remove features that did not work as documented, and fix two configurations that failed at startup. See "Migration Guide (from v1.x to v2.x)" in the README.

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
