// Modules
export { KafkaModule } from './kafka.module';
export { ConsumerModule } from './consumer.module';

// Decorators
export { Consumer, InjectKafkaClient } from './decorators';

// Services
export {
  KafkaClient,
  ConnectionBoundClient,
} from './services/kafka-client.service';
export { TracingService } from './services/tracing.service';

// Health
export { KafkaHealthIndicator } from './health/kafka-health-indicator';

// Interfaces
export * from './interfaces';
