---
'@loipv/nestjs-kafka': minor
---

Support NestJS 12 (ESM and CommonJS applications). Peer ranges for `@nestjs/common`, `@nestjs/core` and `@nestjs/terminus` are now `^11.0.0 || ^12.0.0`. The package stays CommonJS; NestJS 12 apps need Node 20.19+ or 22.12+ (NestJS's own requirement). TypeScript 5.0 or newer is supported; a NestJS 12 app that type-checks libraries (`skipLibCheck: false`) needs TypeScript 5.8+ with `module: nodenext`.
