import { DlqMetricsService } from './dlq-metrics.service';

describe('DlqMetricsService', () => {
  it('tracks global and per-topic counters', () => {
    const m = new DlqMetricsService();
    m.recordHandlerRetry('a');
    m.recordSentToDlq('a', 'a-dlq');
    m.recordReprocessAttempt('a-dlq');
    m.recordReprocessSuccess('a-dlq');
    m.recordFinalFailure('a-dlq', false);

    const metrics = m.getMetrics();
    expect(metrics.global.handlerRetries).toBe(1);
    expect(metrics.global.messagesSentToDlq).toBe(1);
    expect(metrics.global.reprocessFailureRate).toBe(0);
    expect(metrics.byTopic['a'].sentToDlq).toBe(1);
    expect(metrics.byTopic['a-dlq'].dropped).toBe(1);

    m.reset();
    expect(m.getMetrics().global.handlerRetries).toBe(0);
  });
});
