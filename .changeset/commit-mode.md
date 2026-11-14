---
'@loipv/nestjs-kafka': minor
---

Add the `commitMode` consumer option (`'background'` default, `'sync'`). `autoCommit` is deprecated: `autoCommit: false` still commits each message synchronously but logs a warning, because in KafkaJS and confluent-kafka-javascript it means manual commits. Setting `commitMode` and `autoCommit` to conflicting values fails at startup.
