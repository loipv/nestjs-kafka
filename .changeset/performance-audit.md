---
'@loipv/nestjs-kafka': patch
---

Faster consume path: non-JSON message values no longer throw inside deserialization, the idempotency key is extracted once per message, the consumer-lag health check fetches topic offsets in parallel, and revoking partitions clears retry state in one pass.
