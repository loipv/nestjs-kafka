import { PressureManagerService } from './pressure-manager.service';

describe('PressureManagerService', () => {
  it('pauses at threshold and resumes below resume threshold', () => {
    const pmp = new PressureManagerService();
    const consumer = { pause: jest.fn(), resume: jest.fn() };
    pmp.register('g', consumer as any, {
      backPressureThreshold: 80,
      resumeThreshold: 60,
      maxQueueSize: 100,
    });
    pmp.setTopics('g', ['t1']);

    pmp.updateQueueSize('g', 90); // 90% utilization
    expect(consumer.pause).toHaveBeenCalledWith([{ topic: 't1' }]);
    expect(pmp.isPaused('g')).toBe(true);

    pmp.updateQueueSize('g', 50); // 50% utilization
    expect(consumer.resume).toHaveBeenCalledWith([{ topic: 't1' }]);
    expect(pmp.isPaused('g')).toBe(false);
  });
});
