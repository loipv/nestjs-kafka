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

  it('no longer exposes registerHandler or clearConsumers', () => {
    const svc = new ConsumerDiscoveryService();
    expect((svc as any).registerHandler).toBeUndefined();
    expect((svc as any).clearConsumers).toBeUndefined();
  });

  it('skips non-object and prototype-less instances', () => {
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([
      undefined,
      null,
      42,
      'x',
      Object.create(null),
      new TestConsumers(),
    ]);
    expect(svc.getConsumers().map((c) => c.topic)).toEqual(['a']);
  });

  it('never invokes prototype getters', () => {
    const getter = jest.fn(() => {
      throw new Error('side effect');
    });
    class WithGetter {
      @Consumer('g') async g() {}
    }
    Object.defineProperty(WithGetter.prototype, 'boom', { get: getter });

    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([new WithGetter()]);
    expect(svc.getConsumers().map((c) => c.topic)).toEqual(['g']);
    expect(getter).not.toHaveBeenCalled();
  });

  it('reads methods from property descriptors, not through the prototype', () => {
    // A proxied prototype whose get trap throws: listing and descriptors work,
    // property reads do not.
    const proxied = new Proxy(TestConsumers.prototype, {
      get: () => {
        throw new Error('no reads');
      },
    });
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([Object.create(proxied)]);
    expect(svc.getConsumers().map((c) => c.topic)).toEqual(['a']);
  });

  it('skips a prototype that throws on inspection', () => {
    const hostile = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error('no keys');
        },
      },
    );
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([Object.create(hostile), new TestConsumers()]);
    expect(svc.getConsumers().map((c) => c.topic)).toEqual(['a']);
  });

  it('registers an instance listed twice once', () => {
    const instance = new TestConsumers();
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([instance, instance]);
    expect(svc.getConsumers().map((c) => c.topic)).toEqual(['a']);
  });

  class Base {
    @Consumer('base') async h() {}
  }

  it('discovers @Consumer methods declared on a base class', () => {
    class Child extends Base {}
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([new Child()]);
    expect(svc.getConsumers().map((c) => [c.topic, c.methodName])).toEqual([
      ['base', 'h'],
    ]);
  });

  it('an override without @Consumer hides the decorated base method', () => {
    class Child extends Base {
      async h() {}
    }
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([new Child()]);
    expect(svc.getConsumers()).toEqual([]);
  });

  it('stops the walk at a level that throws on inspection', () => {
    class Own {
      @Consumer('own') async own() {}
    }
    const hostile = new Proxy(Object.create(Base.prototype) as object, {
      ownKeys: () => {
        throw new Error('no keys');
      },
    });
    Object.setPrototypeOf(Own.prototype, hostile);
    const svc = new ConsumerDiscoveryService();
    svc.discoverFromProviders([new Own()]);
    expect(svc.getConsumers().map((c) => c.topic)).toEqual(['own']);
  });
});
