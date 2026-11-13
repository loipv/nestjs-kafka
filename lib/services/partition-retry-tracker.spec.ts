import { PartitionRetryTracker } from './partition-retry-tracker';

describe('PartitionRetryTracker', () => {
  let tracker: PartitionRetryTracker;

  beforeEach(() => {
    jest.useFakeTimers();
    tracker = new PartitionRetryTracker();
  });

  afterEach(() => {
    tracker.close();
    jest.useRealTimers();
  });

  it('builds partition and attempt keys', () => {
    expect(PartitionRetryTracker.partitionKey('default:g', 't', 3)).toBe(
      'default:g|t|3',
    );
    expect(PartitionRetryTracker.attemptKey('default:g', 't', 3, '42')).toBe(
      'default:g|t|3|42',
    );
  });

  it('counts attempts per key and restarts after clear', () => {
    expect(tracker.nextAttempt('k')).toBe(1);
    expect(tracker.nextAttempt('k')).toBe(2);
    tracker.clear('k');
    expect(tracker.nextAttempt('k')).toBe(1);
  });

  it('resumes after the delay', () => {
    const resume = jest.fn();
    tracker.scheduleResume('p', 100, resume);
    jest.advanceTimersByTime(99);
    expect(resume).not.toHaveBeenCalled();
    jest.advanceTimersByTime(1);
    expect(resume).toHaveBeenCalledTimes(1);
  });

  it('a second schedule for the same partition replaces the first', () => {
    const first = jest.fn();
    const second = jest.fn();
    tracker.scheduleResume('p', 100, first);
    tracker.scheduleResume('p', 200, second);
    jest.advanceTimersByTime(100);
    expect(first).not.toHaveBeenCalled();
    expect(second).not.toHaveBeenCalled();
    jest.advanceTimersByTime(100);
    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);
  });

  it('swallows a throwing resume', () => {
    tracker.scheduleResume('p', 10, () => {
      throw new Error('Resume can only be called while connected.');
    });
    expect(() => jest.advanceTimersByTime(10)).not.toThrow();
  });

  it('revoke cancels timers and counters of those partitions only', () => {
    const r0 = jest.fn();
    const r1 = jest.fn();
    tracker.nextAttempt('g|t|0|5');
    tracker.nextAttempt('g|t|1|5');
    tracker.scheduleResume('g|t|0', 100, r0);
    tracker.scheduleResume('g|t|1', 100, r1);

    tracker.revoke(['g|t|0']);
    jest.advanceTimersByTime(100);

    expect(r0).not.toHaveBeenCalled();
    expect(r1).toHaveBeenCalledTimes(1);
    expect(tracker.nextAttempt('g|t|0|5')).toBe(1);
    expect(tracker.nextAttempt('g|t|1|5')).toBe(2);
  });

  it('revoke does not touch a partition whose key shares a prefix', () => {
    tracker.nextAttempt('g|t|1|5');
    tracker.nextAttempt('g|t|10|5');
    tracker.revoke(['g|t|1']);
    expect(tracker.nextAttempt('g|t|10|5')).toBe(2);
  });

  it('close cancels everything', () => {
    tracker.scheduleResume('a', 100, jest.fn());
    tracker.scheduleResume('b', 100, jest.fn());
    tracker.nextAttempt('a|1');
    tracker.close();
    expect(jest.getTimerCount()).toBe(0);
    expect(tracker.nextAttempt('a|1')).toBe(1);
  });

  it('close cancels everything and ignores later scheduleResume', () => {
    const before = jest.fn();
    const after = jest.fn();
    tracker.scheduleResume('a', 100, before);
    tracker.close();
    tracker.scheduleResume('b', 100, after);
    jest.advanceTimersByTime(10_000);
    expect(before).not.toHaveBeenCalled();
    expect(after).not.toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);
  });

  it('does not keep the process alive', () => {
    jest.useRealTimers();
    tracker.scheduleResume('p', 60_000, jest.fn());
    const timer = (tracker as any).timers.get('p');
    expect(timer.hasRef()).toBe(false);
  });
});
