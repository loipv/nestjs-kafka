import { Injectable, Logger, Optional, Inject } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { KAFKA_MODULE_OPTIONS, KafkaModuleOptions } from '../interfaces';

type IHeaders = KafkaJS.IHeaders;

// OpenTelemetry types - dynamically imported
type OtelApi = typeof import('@opentelemetry/api');
type Tracer = import('@opentelemetry/api').Tracer;
type Span = import('@opentelemetry/api').Span;
type Context = import('@opentelemetry/api').Context;

// Kafka semantic conventions (OpenTelemetry Semantic Conventions v1.24+)
const SEMATTRS_MESSAGING_SYSTEM = 'messaging.system';
const SEMATTRS_MESSAGING_DESTINATION_NAME = 'messaging.destination.name';
const SEMATTRS_MESSAGING_DESTINATION_PARTITION_ID =
  'messaging.destination.partition.id';
const SEMATTRS_MESSAGING_OPERATION_NAME = 'messaging.operation.name';
const SEMATTRS_MESSAGING_OPERATION_TYPE = 'messaging.operation.type';
const SEMATTRS_MESSAGING_KAFKA_OFFSET = 'messaging.kafka.offset';
const SEMATTRS_MESSAGING_KAFKA_MESSAGE_KEY = 'messaging.kafka.message.key';
const SEMATTRS_MESSAGING_KAFKA_CONSUMER_GROUP =
  'messaging.kafka.consumer.group';
const SEMATTRS_MESSAGING_BATCH_MESSAGE_COUNT = 'messaging.batch.message_count';

// W3C Trace Context header names
const TRACEPARENT_HEADER = 'traceparent';
const TRACESTATE_HEADER = 'tracestate';

export interface TracingOptions {
  /** Enable tracing. Default: false */
  enabled?: boolean;
  /** Custom tracer name. Default: '@loipv/nestjs-kafka' */
  tracerName?: string;
  /** Custom tracer version. Default: package version */
  tracerVersion?: string;
}

interface ProduceSpanOptions {
  topic: string;
  key?: string | null;
  partition?: number;
  headers?: IHeaders;
}

interface ConsumeSpanOptions {
  topic: string;
  partition: number;
  offset: string;
  key?: string | null;
  groupId?: string;
  headers?: IHeaders;
}

interface BatchConsumeSpanOptions {
  topic: string;
  partition: number;
  groupId?: string;
  /** Array of message headers - each message may have different trace context */
  messagesHeaders: Array<{
    offset: string;
    key?: string | null;
    headers?: IHeaders;
  }>;
}

@Injectable()
export class TracingService {
  private readonly logger = new Logger(TracingService.name);
  private otel: OtelApi | null = null;
  private tracer: Tracer | null = null;
  private readonly enabled: boolean;

  constructor(
    @Optional()
    @Inject(KAFKA_MODULE_OPTIONS)
    private readonly options?: KafkaModuleOptions,
  ) {
    this.enabled = options?.tracing?.enabled ?? false;

    if (this.enabled) {
      this.initializeOpenTelemetry();
    }
  }

  private initializeOpenTelemetry(): void {
    try {
      // Dynamic import to avoid errors when OpenTelemetry is not installed
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      this.otel = require('@opentelemetry/api') as OtelApi;

      const tracerName =
        this.options?.tracing?.tracerName ?? '@loipv/nestjs-kafka';
      const tracerVersion = this.options?.tracing?.tracerVersion ?? '0.0.1';

      this.tracer = this.otel.trace.getTracer(tracerName, tracerVersion);
      this.logger.log('OpenTelemetry tracing initialized');
    } catch {
      this.logger.warn(
        'OpenTelemetry not available. Install @opentelemetry/api to enable tracing.',
      );
      this.otel = null;
      this.tracer = null;
    }
  }

  /**
   * Check if tracing is available and enabled
   */
  isEnabled(): boolean {
    return this.enabled && this.tracer !== null;
  }

  /**
   * Create a span for producing a message
   * Returns the span and updated headers with trace context
   *
   * The span is created as a child of the current active span (if any).
   * This means:
   * - If called within an HTTP request span, the publish span will inherit the same trace ID
   * - If no active span exists, a new trace will be created
   */
  startProduceSpan(options: ProduceSpanOptions): {
    span: Span | null;
    headers: IHeaders;
  } {
    if (!this.isEnabled() || !this.otel || !this.tracer) {
      return { span: null, headers: options.headers || {} };
    }

    // Use active context to inherit trace ID from parent span (e.g., HTTP request)
    // If no active span exists, a new trace will be created
    const parentContext = this.otel.context.active();

    const span = this.tracer.startSpan(
      `${options.topic} publish`,
      {
        kind: this.otel.SpanKind.PRODUCER,
        attributes: {
          [SEMATTRS_MESSAGING_SYSTEM]: 'kafka',
          [SEMATTRS_MESSAGING_DESTINATION_NAME]: options.topic,
          [SEMATTRS_MESSAGING_OPERATION_NAME]: 'publish',
          [SEMATTRS_MESSAGING_OPERATION_TYPE]: 'publish',
          ...(options.key && {
            [SEMATTRS_MESSAGING_KAFKA_MESSAGE_KEY]: options.key,
          }),
          ...(options.partition !== undefined && {
            [SEMATTRS_MESSAGING_DESTINATION_PARTITION_ID]: String(
              options.partition,
            ),
          }),
        },
      },
      parentContext,
    );

    // Inject trace context into headers
    const headers: IHeaders = { ...(options.headers || {}) };
    const context = this.otel.trace.setSpan(this.otel.context.active(), span);
    this.injectContext(context, headers);

    return { span, headers };
  }

  /**
   * End a produce span
   */
  endProduceSpan(span: Span | null, error?: Error): void {
    if (!span || !this.otel) return;

    if (error) {
      span.recordException(error);
      span.setStatus({
        code: this.otel.SpanStatusCode.ERROR,
        message: error.message,
      });
    } else {
      span.setStatus({ code: this.otel.SpanStatusCode.OK });
    }

    span.end();
  }

  /**
   * Create a span for consuming a message
   * Extracts trace context from headers to link with producer span
   */
  startConsumeSpan(options: ConsumeSpanOptions): Span | null {
    if (!this.isEnabled() || !this.otel || !this.tracer) {
      return null;
    }

    // Extract trace context from headers
    const parentContext = this.extractContext(options.headers || {});

    // Span name includes groupId for better visibility in tracing UI
    const spanName = options.groupId
      ? `${options.groupId} ${options.topic} process`
      : `${options.topic} process`;

    const span = this.tracer.startSpan(
      spanName,
      {
        kind: this.otel.SpanKind.CONSUMER,
        attributes: {
          [SEMATTRS_MESSAGING_SYSTEM]: 'kafka',
          [SEMATTRS_MESSAGING_DESTINATION_NAME]: options.topic,
          [SEMATTRS_MESSAGING_DESTINATION_PARTITION_ID]: String(
            options.partition,
          ),
          [SEMATTRS_MESSAGING_OPERATION_NAME]: 'process',
          [SEMATTRS_MESSAGING_OPERATION_TYPE]: 'process',
          [SEMATTRS_MESSAGING_KAFKA_OFFSET]: options.offset,
          // Always include groupId attribute (empty string if not provided)
          [SEMATTRS_MESSAGING_KAFKA_CONSUMER_GROUP]: options.groupId || '',
          ...(options.key && {
            [SEMATTRS_MESSAGING_KAFKA_MESSAGE_KEY]: options.key,
          }),
        },
      },
      parentContext,
    );

    return span;
  }

  /**
   * End a consume span
   */
  endConsumeSpan(span: Span | null, error?: Error): void {
    if (!span || !this.otel) return;

    if (error) {
      span.recordException(error);
      span.setStatus({
        code: this.otel.SpanStatusCode.ERROR,
        message: error.message,
      });
    } else {
      span.setStatus({ code: this.otel.SpanStatusCode.OK });
    }

    span.end();
  }

  /**
   * Run a function within a span context
   */
  async withConsumeSpan<T>(
    options: ConsumeSpanOptions,
    fn: () => Promise<T>,
  ): Promise<T> {
    const span = this.startConsumeSpan(options);

    if (!span || !this.otel) {
      return fn();
    }

    const context = this.otel.trace.setSpan(this.otel.context.active(), span);

    try {
      const result = await this.otel.context.with(context, fn);
      this.endConsumeSpan(span);
      return result;
    } catch (error) {
      this.endConsumeSpan(span, error as Error);
      throw error;
    }
  }

  /**
   * Create a span for consuming a batch of messages
   * Uses OpenTelemetry links to connect to all message traces
   *
   * - First message's trace context is used as parent (for trace continuity)
   * - All other messages are added as links (shows relationship without parent-child)
   */
  startBatchConsumeSpan(options: BatchConsumeSpanOptions): Span | null {
    if (!this.isEnabled() || !this.otel || !this.tracer) {
      return null;
    }

    const { topic, partition, groupId, messagesHeaders } = options;
    if (messagesHeaders.length === 0) {
      return null;
    }

    // Extract span contexts from all messages for links
    const links: import('@opentelemetry/api').Link[] = [];
    let parentContext: Context = this.otel.context.active();

    for (let i = 0; i < messagesHeaders.length; i++) {
      const msg = messagesHeaders[i];
      const extractedContext = this.extractContext(msg.headers || {});
      const spanContext = this.otel.trace.getSpanContext(extractedContext);

      if (spanContext && this.otel.isSpanContextValid(spanContext)) {
        if (i === 0) {
          // First message becomes the parent for trace continuity
          parentContext = extractedContext;
        } else {
          // Other messages are added as links
          links.push({
            context: spanContext,
            attributes: {
              [SEMATTRS_MESSAGING_KAFKA_OFFSET]: msg.offset,
              ...(msg.key && {
                [SEMATTRS_MESSAGING_KAFKA_MESSAGE_KEY]: msg.key,
              }),
            },
          });
        }
      }
    }

    const firstMsg = messagesHeaders[0];

    // Span name includes groupId for better visibility in tracing UI
    const spanName = groupId
      ? `${groupId} ${topic} process batch`
      : `${topic} process batch`;

    const span = this.tracer.startSpan(
      spanName,
      {
        kind: this.otel.SpanKind.CONSUMER,
        attributes: {
          [SEMATTRS_MESSAGING_SYSTEM]: 'kafka',
          [SEMATTRS_MESSAGING_DESTINATION_NAME]: topic,
          [SEMATTRS_MESSAGING_DESTINATION_PARTITION_ID]: String(partition),
          [SEMATTRS_MESSAGING_OPERATION_NAME]: 'process',
          [SEMATTRS_MESSAGING_OPERATION_TYPE]: 'process',
          [SEMATTRS_MESSAGING_BATCH_MESSAGE_COUNT]: messagesHeaders.length,
          // Always include groupId attribute (empty string if not provided)
          [SEMATTRS_MESSAGING_KAFKA_CONSUMER_GROUP]: groupId || '',
          ...(firstMsg.key && {
            [SEMATTRS_MESSAGING_KAFKA_MESSAGE_KEY]: firstMsg.key,
          }),
        },
        links, // Links to all other message traces
      },
      parentContext, // First message's trace as parent
    );

    return span;
  }

  /**
   * Run a function within a batch consume span context
   * Links all message traces to the batch processing span
   */
  async withBatchConsumeSpan<T>(
    options: BatchConsumeSpanOptions,
    fn: () => Promise<T>,
  ): Promise<T> {
    const span = this.startBatchConsumeSpan(options);

    if (!span || !this.otel) {
      return fn();
    }

    const context = this.otel.trace.setSpan(this.otel.context.active(), span);

    try {
      const result = await this.otel.context.with(context, fn);
      this.endConsumeSpan(span);
      return result;
    } catch (error) {
      this.endConsumeSpan(span, error as Error);
      throw error;
    }
  }

  /**
   * Inject trace context into Kafka headers (W3C Trace Context format)
   */
  private injectContext(context: Context, headers: IHeaders): void {
    if (!this.otel) return;

    const spanContext = this.otel.trace.getSpan(context)?.spanContext();
    if (!spanContext || !this.otel.isSpanContextValid(spanContext)) {
      return;
    }

    // W3C Trace Context format: version-traceId-spanId-flags
    const traceparent = `00-${spanContext.traceId}-${spanContext.spanId}-${spanContext.traceFlags.toString(16).padStart(2, '0')}`;
    headers[TRACEPARENT_HEADER] = traceparent;

    if (spanContext.traceState) {
      headers[TRACESTATE_HEADER] = spanContext.traceState.serialize();
    }
  }

  /**
   * Extract trace context from Kafka headers
   */
  private extractContext(headers: IHeaders): Context {
    // this.otel is non-null when callers check isEnabled() first, but guard defensively
    if (!this.otel) {
      return {} as Context;
    }

    const traceparent = this.getHeaderValue(headers, TRACEPARENT_HEADER);
    if (!traceparent) {
      return this.otel.context.active();
    }

    // Parse W3C Trace Context: version-traceId-spanId-flags
    const parts = traceparent.split('-');
    if (parts.length !== 4) {
      return this.otel.context.active();
    }

    const [, traceId, spanId, flags] = parts;

    const spanContext: import('@opentelemetry/api').SpanContext = {
      traceId,
      spanId,
      traceFlags: parseInt(flags, 16),
      isRemote: true,
    };

    if (!this.otel.isSpanContextValid(spanContext)) {
      return this.otel.context.active();
    }

    // Parse tracestate if present
    const tracestate = this.getHeaderValue(headers, TRACESTATE_HEADER);
    const finalSpanContext: import('@opentelemetry/api').SpanContext = tracestate
      ? { ...spanContext, traceState: this.otel.createTraceState(tracestate) }
      : spanContext;

    return this.otel.trace.setSpanContext(
      this.otel.context.active(),
      finalSpanContext,
    );
  }

  /**
   * Get header value as string
   */
  private getHeaderValue(headers: IHeaders, key: string): string | undefined {
    const value = headers[key];
    if (!value) return undefined;

    if (Buffer.isBuffer(value)) {
      return value.toString('utf-8');
    }

    if (typeof value === 'string') {
      return value;
    }

    return undefined;
  }
}
