// Smoke test: a Nest 12 ESM app imports the CommonJS library by name (Node's CJS named-export
// detection must see every runtime export) and shares the ESM-only Nest 12 packages with it. It
// also checks that `@Consumer` methods are discovered and the shutdown order. No broker is
// needed: the producer connects lazily and `autoStart: false` leaves consumers unstarted.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import 'reflect-metadata';
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  Consumer,
  ConsumerModule,
  ConsumerRegistryService,
  InjectKafkaClient,
  KafkaClient,
  KafkaHealthIndicator,
  KafkaModule,
  TracingService,
} from '@loipv/nestjs-kafka';

const require = createRequire(import.meta.url);
const timeout = (ms, message) =>
  new Promise((_, reject) => setTimeout(() => reject(new Error(message)), ms).unref());

// Record discovery and the shutdown hooks, in call order.
const calls = [];
const { registerConsumers, onModuleDestroy } = ConsumerRegistryService.prototype;
ConsumerRegistryService.prototype.registerConsumers = function (consumers) {
  calls.push(`register:${consumers.map((c) => c.topic).join(',')}`);
  return registerConsumers.call(this, consumers);
};
ConsumerRegistryService.prototype.onModuleDestroy = function () {
  calls.push('registry.onModuleDestroy');
  return onModuleDestroy.call(this);
};
const { onApplicationShutdown } = KafkaClient.prototype;
KafkaClient.prototype.onApplicationShutdown = function () {
  calls.push('client.onApplicationShutdown');
  return onApplicationShutdown.call(this);
};

class Handlers {
  onSmoke() {}
}
// No decorator syntax without a compiler: apply the decorators by hand.
Consumer('smoke')(
  Handlers.prototype,
  'onSmoke',
  Object.getOwnPropertyDescriptor(Handlers.prototype, 'onSmoke'),
);
Injectable()(Handlers);

class AppHook {
  onModuleDestroy() {
    calls.push('app.onModuleDestroy');
  }
}
Injectable()(AppHook);

class AppModule {}
Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'smoke',
      brokers: ['127.0.0.1:1'],
      tracing: { enabled: true },
    }),
    ConsumerModule.forRoot({ autoStart: false }),
  ],
  providers: [Handlers, AppHook],
})(AppModule);

for (const [name, value] of Object.entries({
  Consumer,
  ConsumerModule,
  ConsumerRegistryService,
  InjectKafkaClient,
  KafkaClient,
  KafkaHealthIndicator,
  KafkaModule,
  TracingService,
})) {
  assert.equal(typeof value, 'function', `${name} is not exported as a function`);
}
assert.match(require('@nestjs/core/package.json').version, /^12\./);

// Every CommonJS export, not only the ones named above, must be importable by name from ESM.
const cjsExports = Object.keys(require('@loipv/nestjs-kafka'));
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
// First call of each: Nest calls a provider's hook once per token that exposes the instance,
// and the default connection's client token is the KafkaClient itself (Nest 11 too).
assert.deepEqual([...new Set(calls)], [
  'register:smoke',
  'app.onModuleDestroy',
  'registry.onModuleDestroy',
  'client.onApplicationShutdown',
]);
console.log('SMOKE OK');
