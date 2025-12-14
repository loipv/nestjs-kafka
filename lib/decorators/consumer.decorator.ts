import { SetMetadata } from '@nestjs/common';
import { ConsumerOptions } from '../interfaces';
import { KAFKA_CONSUMER_METADATA } from './constants';

export interface ConsumerMethodMetadata {
  topic: string;
  options: Partial<ConsumerOptions>;
}

export function Consumer(
  topic: string,
  options?: Partial<ConsumerOptions>,
): MethodDecorator {
  return (
    target: object,
    propertyKey: string | symbol,
    descriptor: PropertyDescriptor,
  ) => {
    const metadata: ConsumerMethodMetadata = {
      topic,
      options: options || {},
    };

    SetMetadata(KAFKA_CONSUMER_METADATA, metadata)(
      target,
      propertyKey,
      descriptor,
    );

    return descriptor;
  };
}

export type MessageHandler<T = any> = (message: T) => Promise<void>;
export type BatchHandler<T = any> = (messages: T[]) => Promise<void>;
