import { BatchProcessorService } from './batch-processor.service';

describe('BatchProcessorService', () => {
  const svc = new BatchProcessorService();
  describe('splitIntoBatches', () => {
    it('splits into chunks of batchSize', () => {
      expect(svc.splitIntoBatches([1, 2, 3, 4, 5], 2)).toEqual([
        [1, 2],
        [3, 4],
        [5],
      ]);
      expect(svc.splitIntoBatches([], 2)).toEqual([]);
    });

    it.each([undefined, 0, -3, 0.5, 2.5])(
      'falls back to 100 for batchSize %p',
      (size) => {
        const items = Array.from({ length: 150 }, (_, i) => i);
        expect(svc.splitIntoBatches(items, size).map((c) => c.length)).toEqual([
          100, 50,
        ]);
      },
    );
  });

  describe('groupMessagesByKey', () => {
    it('groups by key, null key under sentinel', () => {
      const grouped = svc.groupMessagesByKey([
        { key: Buffer.from('a') } as any,
        { key: null } as any,
        { key: Buffer.from('a') } as any,
      ]);
      expect(grouped).toHaveLength(2);
      expect(grouped.find((g) => g.key === 'a')!.messages).toHaveLength(2);
      expect(
        grouped.find((g) => g.key === '__null_key__')!.messages,
      ).toHaveLength(1);
    });
  });
});
