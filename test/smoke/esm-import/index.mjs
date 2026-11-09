// Smoke test: an ESM app imports the CommonJS library by name (Node's CJS named-export
// detection must see every runtime export). No broker is needed.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import 'reflect-metadata';
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  Consumer,
  ConsumerModule,
  InjectKafkaClient,
  KafkaClient,
  KafkaHealthIndicator,
  KafkaModule,
  TracingService,
} from '@loipv/nestjs-kafka';

const timeout = (ms, message) =>
  new Promise((_, reject) => setTimeout(() => reject(new Error(message)), ms).unref());

class Handlers {
  onSmoke() {}
}
// No decorator syntax without a compiler: apply the decorators by hand.
Consumer('smoke', { disabled: true })(
  Handlers.prototype,
  'onSmoke',
  Object.getOwnPropertyDescriptor(Handlers.prototype, 'onSmoke'),
);
Injectable()(Handlers);

class AppModule {}
Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'smoke',
      brokers: ['127.0.0.1:1'],
      tracing: { enabled: true },
    }),
    ConsumerModule.forRoot(),
  ],
  providers: [Handlers],
})(AppModule);

for (const [name, value] of Object.entries({
  Consumer,
  ConsumerModule,
  InjectKafkaClient,
  KafkaClient,
  KafkaHealthIndicator,
  KafkaModule,
  TracingService,
})) {
  assert.equal(typeof value, 'function', `${name} is not exported as a function`);
}

// Every CommonJS export, not only the ones named above, must be importable by name from ESM.
const cjsExports = Object.keys(createRequire(import.meta.url)('@loipv/nestjs-kafka'));
const esmExports = new Set(Object.keys(await import('@loipv/nestjs-kafka')));
assert.deepEqual(
  cjsExports.filter((name) => !esmExports.has(name)),
  [],
  'CommonJS exports not visible as named ESM exports',
);

const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
let failure;
try {
  assert.ok(app.get(KafkaClient) instanceof KafkaClient);
} catch (err) {
  failure = err;
}
// Close in every case, but a close error must not hide the assertion error.
try {
  await Promise.race([app.close(), timeout(30_000, 'app.close() hung')]);
} catch (closeErr) {
  if (failure === undefined) throw closeErr;
  console.error('app.close() failed as well:', closeErr);
}
if (failure !== undefined) throw failure;
console.log('SMOKE OK');
