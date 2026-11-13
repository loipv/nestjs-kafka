import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { InjectKafkaClient } from './inject-kafka-client.decorator';
import { KafkaModule } from '../kafka.module';
import {
  ConnectionBoundClient,
  KafkaClient,
} from '../services/kafka-client.service';

// The native librdkafka binding is not installed under `npm ci --ignore-scripts` (CI, release).
jest.mock('@confluentinc/kafka-javascript');

@Injectable()
class DefaultUser {
  constructor(@InjectKafkaClient() readonly client: KafkaClient) {}
}

@Injectable()
class AnalyticsUser {
  constructor(
    @InjectKafkaClient('analytics') readonly client: ConnectionBoundClient,
  ) {}
}

const A = { clientId: 'a', brokers: ['localhost:9092'] };
const B = { name: 'analytics', clientId: 'b', brokers: ['localhost:9093'] };

describe('InjectKafkaClient', () => {
  let mod: TestingModule | undefined;

  afterEach(async () => {
    await mod?.close();
    mod = undefined;
  });

  it('injects the default client by default', async () => {
    mod = await Test.createTestingModule({
      imports: [KafkaModule.forRoot(A)],
      providers: [DefaultUser],
    }).compile();
    expect(mod.get(DefaultUser).client).toBe(mod.get(KafkaClient));
  });

  it('injects a ConnectionBoundClient for a named connection', async () => {
    mod = await Test.createTestingModule({
      imports: [KafkaModule.forRoot(A), KafkaModule.forRoot(B)],
      providers: [AnalyticsUser],
    }).compile();
    const send = jest
      .spyOn(KafkaClient.prototype, 'send')
      .mockResolvedValue(undefined);
    try {
      const { client } = mod.get(AnalyticsUser);
      expect(client).toBeInstanceOf(ConnectionBoundClient);
      await client.send('t', { value: 'v' });
      expect(send).toHaveBeenCalledWith(
        't',
        { value: 'v' },
        { connection: 'analytics' },
      );
    } finally {
      send.mockRestore();
    }
  });
});
