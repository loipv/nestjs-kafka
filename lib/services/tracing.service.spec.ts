import { BasicTracerProvider } from '@opentelemetry/sdk-trace-base';
import {
  CompositePropagator,
  W3CBaggagePropagator,
  W3CTraceContextPropagator,
} from '@opentelemetry/core';
import {
  Context,
  TextMapPropagator,
  context,
  propagation,
  trace,
} from '@opentelemetry/api';
import { TracingService } from './tracing.service';

beforeAll(() => {
  // Real spans, not no-op ProxyTracer spans (OTel 2.x: set via global setter)
  trace.setGlobalTracerProvider(new BasicTracerProvider());
});

beforeEach(() => {
  // What an OpenTelemetry SDK registers by default
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
});

afterEach(() => {
  propagation.disable();
});

const tp = `00-${'a'.repeat(32)}-${'b'.repeat(16)}-01`;
const enabled = () => {
  const svc = new TracingService();
  svc.configure({ enabled: true });
  return svc;
};
const consumeTraceId = (svc: TracingService, headers: any) =>
  svc
    .startConsumeSpan({ topic: 't', partition: 0, offset: '1', headers })
    ?.spanContext().traceId;

describe('TracingService', () => {
  it('is disabled by default and passes headers through', () => {
    const svc = new TracingService();
    svc.configure(undefined);
    expect(svc.isEnabled()).toBe(false);
    const { span, headers } = svc.startProduceSpan({
      topic: 't',
      headers: { a: 'b' },
    });
    expect(span).toBeNull();
    expect(headers).toEqual({ a: 'b' });
  });

  it('injects W3C traceparent when enabled', () => {
    const svc = new TracingService();
    svc.configure({ enabled: true });
    expect(svc.isEnabled()).toBe(true);
    const { span, headers } = svc.startProduceSpan({ topic: 't' });
    expect(span).not.toBeNull();
    expect(headers['traceparent']).toMatch(
      /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/,
    );
  });

  it('withConsumeSpan links the producer trace from headers and propagates errors', async () => {
    const svc = new TracingService();
    svc.configure({ enabled: true });
    const traceparent = `00-${'a'.repeat(32)}-${'b'.repeat(16)}-01`;

    // Consumer span must inherit the producer's traceId from the header
    const span = svc.startConsumeSpan({
      topic: 't',
      partition: 0,
      offset: '1',
      headers: { traceparent },
    });
    expect(span?.spanContext().traceId).toBe('a'.repeat(32));

    // withConsumeSpan awaits fn and rethrows (span records the error)
    await expect(
      svc.withConsumeSpan(
        { topic: 't', partition: 0, offset: '1', headers: { traceparent } },
        async () => {
          throw new Error('boom');
        },
      ),
    ).rejects.toThrow('boom');
  });

  it('links the producer trace from a Buffer traceparent', () => {
    expect(consumeTraceId(enabled(), { traceparent: Buffer.from(tp) })).toBe(
      'a'.repeat(32),
    );
  });

  it('links the producer trace from an array header', () => {
    expect(consumeTraceId(enabled(), { traceparent: [Buffer.from(tp)] })).toBe(
      'a'.repeat(32),
    );
  });

  it('starts a new trace without a traceparent header', () => {
    const traceId = consumeTraceId(enabled(), {});
    expect(traceId).toMatch(/^[0-9a-f]{32}$/);
    expect(traceId).not.toBe('a'.repeat(32));
  });

  it("injects through the application's propagator", () => {
    const custom: TextMapPropagator = {
      inject: (_ctx, carrier: any, setter) =>
        setter.set(carrier, 'x-custom', '1'),
      extract: (ctx: Context) => ctx,
      fields: () => ['x-custom'],
    };
    propagation.disable();
    propagation.setGlobalPropagator(custom);
    const { headers } = enabled().startProduceSpan({ topic: 't' });
    expect(headers['x-custom']).toBe('1');
    expect(headers['traceparent']).toBeUndefined();
  });

  it('carries baggage across produce and consume', () => {
    propagation.disable();
    propagation.setGlobalPropagator(
      new CompositePropagator({
        propagators: [
          new W3CTraceContextPropagator(),
          new W3CBaggagePropagator(),
        ],
      }),
    );
    // No context manager is registered in unit tests, so drive the private
    // inject/extract pair directly with an explicit context.
    const svc = enabled() as any;
    const withBaggage = propagation.setBaggage(
      context.active(),
      propagation.createBaggage({ k: { value: 'v' } }),
    );
    const headers: Record<string, string> = {};
    svc.injectContext(withBaggage, headers);
    expect(headers['baggage']).toBe('k=v');

    const extracted = svc.extractContext({
      baggage: Buffer.from(headers['baggage']),
    }) as Context;
    expect(propagation.getBaggage(extracted)?.getEntry('k')?.value).toBe('v');
  });

  it('first enabling configure() wins; later calls are ignored', () => {
    const svc = new TracingService();
    svc.configure(undefined);
    expect(svc.isEnabled()).toBe(false);
    svc.configure({ enabled: true });
    svc.configure({ enabled: false });
    expect(svc.isEnabled()).toBe(true);
  });
});
