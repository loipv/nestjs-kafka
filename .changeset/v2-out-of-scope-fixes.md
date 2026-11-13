---
'@loipv/nestjs-kafka': major
---

Fix a startup failure with aliased consumer providers, make shutdown and startup order deterministic, and close the remaining DLQ, commit and health edge cases. See "Fixed", "Changed behaviour" and "Removed" in the README's "Migration Guide (from v1.x to v2.x)".

Fixes:

- A consumer provider also exposed through `useExisting` no longer fails startup with "already has a handler for topic".
- `@Consumer` methods declared on a base class are discovered; request-scoped consumer providers are skipped with a warning instead of registering a handler without its dependencies.
- Consumers always stop before producers disconnect, whatever the import order of `KafkaModule` and `ConsumerModule`.
- A partially failed DLQ send only re-sends the messages that did not arrive.
- A synchronous commit (`autoCommit: false`) waits at most 10s, and a partition revoked during a delivery is not committed by the old owner.
- `checkConsumerLag` lists topics whose offsets cannot be read in `unreadableTopics` instead of counting them as lag 0.

Features:

- `ConsumerModule.forRoot({ autoStart: false })` with `ConsumerRegistryService.startAll()` (now exported) to start consumers after the application is ready.
- `checkBrokers` / `checkConsumerLag` accept a connection name and reuse one admin client per connection.

Breaking changes:

- `KAFKA_MODULE_OPTIONS` is removed: inject `getKafkaOptionsToken(name)` (`getKafkaOptionsToken()` for the default connection).
- `ConsumerRegistryService` stops consumers in `onModuleDestroy` (was `onApplicationShutdown`).
- `ConsumerRegistryService.startAll()` is memoised and closes started consumers and producers before rejecting.
- DLQ sends go out one message per send; `DlqService.handleFailure()` / `sendToDlq()` take an optional `sent` set.
