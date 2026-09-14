import { BasicTracerProvider } from '@opentelemetry/sdk-trace-base';
import { trace } from '@opentelemetry/api';
import { TracingService } from './tracing.service';

beforeAll(() => {
  // Real spans, not no-op ProxyTracer spans (OTel 2.x: set via global setter)
  trace.setGlobalTracerProvider(new BasicTracerProvider());
});

describe('TracingService', () => {
  it('is disabled by default and passes headers through', () => {
    const svc = new TracingService(undefined);
    expect(svc.isEnabled()).toBe(false);
    const { span, headers } = svc.startProduceSpan({
      topic: 't',
      headers: { a: 'b' },
    });
    expect(span).toBeNull();
    expect(headers).toEqual({ a: 'b' });
  });

  it('injects W3C traceparent when enabled', () => {
    const svc = new TracingService({ tracing: { enabled: true } } as any);
    expect(svc.isEnabled()).toBe(true);
    const { span, headers } = svc.startProduceSpan({ topic: 't' });
    expect(span).not.toBeNull();
    expect(headers['traceparent']).toMatch(
      /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/,
    );
  });

  it('withConsumeSpan links the producer trace from headers and propagates errors', async () => {
    const svc = new TracingService({ tracing: { enabled: true } } as any);
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
});
