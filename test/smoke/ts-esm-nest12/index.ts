// Smoke test: a TypeScript 5.8 ESM app (`module: nodenext`) on Nest 12 type-checks against the
// published CommonJS declarations (`skipLibCheck: false` makes tsc read them) and runs. It
// covers real decorator syntax, a named connection, the Terminus result type, discovery and
// the shutdown order.
// No broker is needed: the producer connects lazily and `autoStart: false` leaves consumers
// unstarted.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import 'reflect-metadata';
import { Injectable, Module, OnModuleDestroy } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { HealthIndicatorResult } from '@nestjs/terminus';
import {
  ConnectionBoundClient,
  Consumer,
  ConsumerModule,
  ConsumerRegistryService,
  InjectKafkaClient,
  KafkaClient,
  KafkaHealthIndicator,
  KafkaModule,
} from '@loipv/nestjs-kafka';
import type {
  ConsumerMetadata,
  KafkaMessagePayload,
} from '@loipv/nestjs-kafka';

const require = createRequire(import.meta.url);
const timeout = (ms: number, message: string) =>
  new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error(message)), ms).unref(),
  );

// Record discovery and the shutdown hooks, in call order.
const calls: string[] = [];
const registry = ConsumerRegistryService.prototype;
const { registerConsumers, onModuleDestroy } = registry;
registry.registerConsumers = function (
  this: ConsumerRegistryService,
  consumers: ConsumerMetadata[],
) {
  calls.push(`register:${consumers.map((c) => c.topic).join(',')}`);
  return registerConsumers.call(this, consumers);
};
registry.onModuleDestroy = function (this: ConsumerRegistryService) {
  calls.push('registry.onModuleDestroy');
  return onModuleDestroy.call(this);
};
const { onApplicationShutdown } = KafkaClient.prototype;
KafkaClient.prototype.onApplicationShutdown = function (this: KafkaClient) {
  calls.push('client.onApplicationShutdown');
  return onApplicationShutdown.call(this);
};

@Injectable()
class Handlers {
  @Consumer('smoke')
  async onSmoke(message: KafkaMessagePayload): Promise<void> {
    void message;
  }
}

@Injectable()
class Analytics {
  constructor(
    @InjectKafkaClient('analytics') readonly client: ConnectionBoundClient,
  ) {}
}

@Injectable()
class AppHook implements OnModuleDestroy {
  onModuleDestroy(): void {
    calls.push('app.onModuleDestroy');
  }
}

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'smoke',
      brokers: ['127.0.0.1:1'],
      tracing: { enabled: true },
    }),
    KafkaModule.forRoot({
      name: 'analytics',
      clientId: 'smoke-analytics',
      brokers: ['127.0.0.1:1'],
    }),
    ConsumerModule.forRoot({ autoStart: false }),
  ],
  providers: [Handlers, Analytics, AppHook],
})
class AppModule {}

assert.match(require('@nestjs/core/package.json').version, /^12\./);

const app = await NestFactory.createApplicationContext(AppModule, {
  logger: false,
});
let failure: unknown;
try {
  assert.ok(app.get(KafkaClient) instanceof KafkaClient);
  assert.ok(app.get(Analytics).client, 'named connection client not injected');
  const health: HealthIndicatorResult = app
    .get(KafkaHealthIndicator)
    .isHealthy('kafka');
  assert.ok(
    ['up', 'down'].includes(health.kafka.status),
    `status ${health.kafka.status}`,
  );
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
// First call of each: Nest calls a provider's hook once per token that exposes the instance.
assert.deepEqual(
  [...new Set(calls)],
  [
    'register:smoke',
    'app.onModuleDestroy',
    'registry.onModuleDestroy',
    'client.onApplicationShutdown',
  ],
);
console.log('SMOKE OK');
