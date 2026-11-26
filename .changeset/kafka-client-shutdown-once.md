---
'@loipv/nestjs-kafka': patch
---

`KafkaClient`'s shutdown hook runs once: the default connection's client token is now an alias of `KafkaClient` (it was a factory returning the same instance, so Nest ran the hook twice; the second producer disconnect did nothing).
