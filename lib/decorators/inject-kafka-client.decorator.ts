import { Inject } from '@nestjs/common';
import { DEFAULT_KAFKA_CONNECTION, getKafkaClientToken } from '../interfaces';

/**
 * Decorator to inject a KafkaClient for a specific connection.
 *
 * @param connectionName - The name of the Kafka connection (default: 'default')
 *
 * @example
 * ```typescript
 * @Injectable()
 * export class OrderService {
 *   constructor(
 *     @InjectKafkaClient() private readonly kafka: KafkaClient,
 *     @InjectKafkaClient('analytics') private readonly analyticsKafka: KafkaClient,
 *   ) {}
 * }
 * ```
 */
export function InjectKafkaClient(
  connectionName: string = DEFAULT_KAFKA_CONNECTION,
): ParameterDecorator {
  return Inject(getKafkaClientToken(connectionName));
}
