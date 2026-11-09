// Smoke test: the packed library loads and runs without its optional peers
// (`@nestjs/terminus`, `@opentelemetry/api`). No broker is needed.
const assert = require('node:assert/strict');
require('reflect-metadata');
const { Injectable, Module } = require('@nestjs/common');
const { NestFactory } = require('@nestjs/core');
const { Consumer, ConsumerModule, KafkaClient, KafkaModule } = require('@loipv/nestjs-kafka');

const timeout = (ms, message) =>
  new Promise((_, reject) => setTimeout(() => reject(new Error(message)), ms).unref());

class Handlers {
  onSmoke() {}
}
// Plain JS has no decorator syntax: apply the decorators by hand.
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
      tracing: { enabled: false },
    }),
    ConsumerModule.forRoot(),
  ],
  providers: [Handlers],
})(AppModule);

async function main() {
  // The fixture must really lack the optional peers, or it proves nothing.
  for (const peer of ['@nestjs/terminus', '@opentelemetry/api']) {
    assert.throws(() => require.resolve(peer), { code: 'MODULE_NOT_FOUND' }, `${peer} is installed`);
  }

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
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
