import { Global, Module } from '@nestjs/common';
import { KafkaCoreService } from './services/kafka-core.service';
import { KafkaClient } from './services/kafka-client.service';
import { TracingService } from './services/tracing.service';
import { KafkaHealthIndicator } from './health/kafka-health-indicator';

const SHARED_PROVIDERS = [
  KafkaCoreService,
  KafkaClient,
  KafkaHealthIndicator,
  TracingService,
];

/**
 * Shared Kafka infrastructure. A static module, so Nest creates it once no
 * matter how many `KafkaModule.forRoot*()` calls import it: every connection
 * registers into the same KafkaCoreService.
 */
@Global()
@Module({
  providers: SHARED_PROVIDERS,
  exports: SHARED_PROVIDERS,
})
export class KafkaCoreModule {}
