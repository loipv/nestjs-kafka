import { CircuitBreakerService, CircuitState } from './circuit-breaker.service';

describe('CircuitBreakerService', () => {
  it('opens after failureThreshold consecutive failures', () => {
    const cb = new CircuitBreakerService();
    for (let i = 0; i < 5; i++) cb.recordFailure('k');
    expect(cb.getState('k')).toBe(CircuitState.OPEN);
    expect(cb.canExecute('k')).toBe(false);
  });

  it('half-opens after resetTimeout, closes after successes', () => {
    jest.useFakeTimers();
    const cb = new CircuitBreakerService();
    for (let i = 0; i < 5; i++) cb.recordFailure('k');
    jest.advanceTimersByTime(30_001);
    expect(cb.canExecute('k')).toBe(true);
    expect(cb.getState('k')).toBe(CircuitState.HALF_OPEN);
    for (let i = 0; i < 3; i++) cb.recordSuccess('k');
    expect(cb.getState('k')).toBe(CircuitState.CLOSED);
    jest.useRealTimers();
  });

  it('reopens on any failure in half-open', () => {
    jest.useFakeTimers();
    const cb = new CircuitBreakerService();
    for (let i = 0; i < 5; i++) cb.recordFailure('k');
    jest.advanceTimersByTime(30_001);
    cb.canExecute('k');
    cb.recordFailure('k');
    expect(cb.getState('k')).toBe(CircuitState.OPEN);
    jest.useRealTimers();
  });
});
