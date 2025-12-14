import { Injectable, Logger } from '@nestjs/common';
import { Consumer } from 'kafkajs';
import { PressureState, PressureManagerOptions } from '../interfaces';

@Injectable()
export class PressureManagerService {
  private readonly logger = new Logger(PressureManagerService.name);

  private states = new Map<string, PressureState>();
  private consumers = new Map<string, Consumer>();
  private options = new Map<string, PressureManagerOptions>();

  register(
    consumerId: string,
    consumer: Consumer,
    opts: PressureManagerOptions,
  ): void {
    this.consumers.set(consumerId, consumer);
    this.options.set(consumerId, opts);
    this.states.set(consumerId, {
      isPaused: false,
      currentQueueSize: 0,
      maxQueueSize: opts.maxQueueSize,
      currentConcurrency: 0,
      maxConcurrency: opts.backPressureThreshold,
      utilizationPercent: 0,
    });
  }

  updateQueueSize(consumerId: string, size: number): void {
    const state = this.states.get(consumerId);
    const opts = this.options.get(consumerId);

    if (!state || !opts) return;

    state.currentQueueSize = size;
    state.utilizationPercent = (size / state.maxQueueSize) * 100;

    this.checkPressure(consumerId);
  }

  updateConcurrency(consumerId: string, delta: number): void {
    const state = this.states.get(consumerId);
    if (!state) return;

    state.currentConcurrency += delta;
    this.checkPressure(consumerId);
  }

  private checkPressure(consumerId: string): void {
    const state = this.states.get(consumerId);
    const consumer = this.consumers.get(consumerId);
    const opts = this.options.get(consumerId);

    if (!state || !consumer || !opts) return;

    const shouldPause = state.utilizationPercent >= opts.backPressureThreshold;
    const shouldResume = state.utilizationPercent <= opts.resumeThreshold;

    if (shouldPause && !state.isPaused) {
      this.logger.warn(
        `Back pressure triggered for ${consumerId}, pausing consumer`,
      );
      consumer.pause([{ topic: '*' }]);
      state.isPaused = true;
    } else if (shouldResume && state.isPaused) {
      this.logger.log(`Resuming consumer ${consumerId}`);
      consumer.resume([{ topic: '*' }]);
      state.isPaused = false;
    }
  }

  getState(consumerId: string): PressureState | undefined {
    return this.states.get(consumerId);
  }

  isPaused(consumerId: string): boolean {
    return this.states.get(consumerId)?.isPaused ?? false;
  }
}
