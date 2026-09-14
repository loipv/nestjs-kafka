import { ConsumerDiscoveryService } from './consumer-discovery.service';
import { Consumer } from '../decorators/consumer.decorator';

class TestConsumers {
  @Consumer('a') async a() {}
  @Consumer('b', { disabled: true }) async b() {}
  plain() {}
}

describe('ConsumerDiscoveryService', () => {
  it('discovers @Consumer methods, skips disabled and plain methods', () => {
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([new TestConsumers()]);
    expect(svc.getConsumers().map((c) => c.topic)).toEqual(['a']);
  });

  it('clearConsumers resets', () => {
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([new TestConsumers()]);
    svc.clearConsumers();
    expect(svc.getConsumers()).toHaveLength(0);
  });
});
