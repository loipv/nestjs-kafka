// Smoke test: a Nest 11 CommonJS app `require()`s the packed library with every optional peer
// installed. No broker is needed: the producer connects lazily on the first send.
const assert = require('node:assert/strict');
require('reflect-metadata');
const { Injectable, Module } = require('@nestjs/common');
const { NestFactory } = require('@nestjs/core');
const {
  Consumer,
  ConsumerModule,
  InjectKafkaClient,
  KafkaClient,
  KafkaHealthIndicator,
  KafkaModule,
  TracingService,
} = require('@loipv/nestjs-kafka');

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
      tracing: { enabled: true },
    }),
    ConsumerModule.forRoot(),
  ],
  providers: [Handlers],
})(AppModule);

async function main() {
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
