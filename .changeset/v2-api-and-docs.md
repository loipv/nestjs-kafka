---
'@loipv/nestjs-kafka': major
---

Make `autoCommit: false` commit, remove `KafkaModule.forRootMultiple()`, bound timer options, trace through the application's OpenTelemetry propagator, and ship JSDoc in the published types. See "Changed behaviour" and "Removed" in the README's "Migration Guide (from v1.x to v2.x)".

Fixes:

- `autoCommit: false` commits each handled message (or batch chunk) synchronously; offsets used to be stored but never committed, so the group re-read everything after a restart.
- `retry.initialRetryTime`, `retry.maxRetryTime` and `dlq.sendTimeout` above 2147483647 ms fail at startup instead of firing after 1 ms (a DLQ send then always timed out and the message was retried forever).
- A `ConsumerModule.forRoot()` default is no longer hidden by an explicit `undefined` in `@Consumer()` options.
- Consumer discovery no longer reads provider getters, which could trigger their side effects.
- Producers of several connections disconnect in parallel on shutdown.
- The published type declarations keep their JSDoc.

Breaking changes:

- `KafkaModule.forRootMultiple()` is removed: call `KafkaModule.forRoot()` once per connection.
- A second registration of a connection name with different options fails at startup (it used to be ignored with a warning).
- Timer options must be positive (`retry.initialRetryTime: 0` now fails at startup).
- Trace context goes through the registered OpenTelemetry propagator; without one, no trace headers are written.
- `KafkaHealthIndicator` no longer has an optional `HealthIndicatorService` constructor parameter (it was never injected); it returns plain results, which Terminus evaluates as before.
