---
'@loipv/nestjs-kafka': major
---

Retry failed messages without blocking the consumer, and fix message loss, data corruption, batch sizing, startup order, health and TLS. See "Changed behaviour" in the README's "Migration Guide (from v1.x to v2.x)".

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
- `retry.maxRetryTime` (default 30000) caps every retry delay, DLQ backoff included.
- A failed DLQ send is retried with the backoff delay; new `dlq.sendTimeout` (default 30000) bounds each send.
- Batch consumers dead-letter after `dlq.maxRetries` (was `retry.retries`), in one send per failing chunk, and commit after each successful chunk.
- `KafkaHealthIndicator.isHealthy` is up only when every producer is connected and every consumer group is running; its details are `{ producers, consumers }` instead of `connected`. Producers connect at startup.
- An `ssl` object with an unknown key fails at startup. New `rdkafka` option passes raw librdkafka properties.
- `DlqService.handleFailure()` and `sendToDlq()` take `messages: KafkaMessage[]`; `handleFailure()` never rejects. `IdempotencyService` methods take a `namespace` argument. `KafkaHealthIndicator` no longer depends on `KafkaClient`.
