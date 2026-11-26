// Smoke test: a Nest 12 CommonJS app `require()`s the packed library (which reaches the ESM-only
// Nest 12 packages through `require(esm)`) with every optional peer installed. It also checks
// that `@Consumer` methods are discovered and the shutdown order. No broker is needed: the
// producer connects lazily and `autoStart: false` leaves consumers unstarted.
const assert = require('node:assert/strict');
require('reflect-metadata');
const { Injectable, Module } = require('@nestjs/common');
const { NestFactory } = require('@nestjs/core');
const {
  Consumer,
  ConsumerModule,
  ConsumerRegistryService,
  InjectKafkaClient,
  KafkaClient,
  KafkaHealthIndicator,
  KafkaModule,
  TracingService,
} = require('@loipv/nestjs-kafka');

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
// Plain JS has no decorator syntax: apply the decorators by hand.
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

async function main() {
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
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
