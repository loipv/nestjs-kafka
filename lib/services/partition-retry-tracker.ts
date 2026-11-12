import { Logger } from '@nestjs/common';

/**
 * In-memory retry state for pause-and-retry: per-message attempt counters and
 * one resume timer per paused partition. Nothing here sleeps; a handler that
 * fails pauses its partition, schedules a resume and rethrows.
 *
 * Counters are lost on restart or when a partition moves to another member;
 * the retry ladder then starts again (the message itself is never lost).
 */
export class PartitionRetryTracker {
  private readonly logger = new Logger(PartitionRetryTracker.name);
  private readonly attempts = new Map<string, number>();
  private readonly timers = new Map<string, NodeJS.Timeout>();

  static partitionKey(
    groupKey: string,
    topic: string,
    partition: number,
  ): string {
    return `${groupKey}|${topic}|${partition}`;
  }

  static attemptKey(
    groupKey: string,
    topic: string,
    partition: number,
    offset: string,
  ): string {
    return `${PartitionRetryTracker.partitionKey(groupKey, topic, partition)}|${offset}`;
  }

  /** Records one more attempt for `attemptKey` and returns it (1-based). */
  nextAttempt(attemptKey: string): number {
    const attempt = (this.attempts.get(attemptKey) ?? 0) + 1;
    this.attempts.set(attemptKey, attempt);
    return attempt;
  }

  clear(attemptKey: string): void {
    this.attempts.delete(attemptKey);
  }

  /** Calls `resume` after `delayMs`, replacing a pending resume for the same partition. */
  scheduleResume(
    partitionKey: string,
    delayMs: number,
    resume: () => void,
  ): void {
    this.cancelTimer(partitionKey);
    const timer = setTimeout(() => {
      this.timers.delete(partitionKey);
      try {
        resume();
      } catch (error) {
        this.logger.debug(`Resume of ${partitionKey} failed: ${String(error)}`);
      }
    }, delayMs);
    timer.unref();
    this.timers.set(partitionKey, timer);
  }

  /** Forgets the timers and counters of revoked partitions. */
  revoke(partitionKeys: string[]): void {
    for (const partitionKey of partitionKeys) {
      this.cancelTimer(partitionKey);
      const prefix = `${partitionKey}|`;
      for (const key of [...this.attempts.keys()]) {
        if (key.startsWith(prefix)) this.attempts.delete(key);
      }
    }
  }

  clearAll(): void {
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
    this.attempts.clear();
  }

  private cancelTimer(partitionKey: string): void {
    const timer = this.timers.get(partitionKey);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(partitionKey);
    }
  }
}
