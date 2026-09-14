import { BatchProcessorService } from './batch-processor.service';

describe('BatchProcessorService', () => {
  const svc = new BatchProcessorService();
  const mkMsg = (i: number) =>
    ({
      offset: String(i),
      key: null,
      value: Buffer.from(String(i)),
      headers: {},
      timestamp: '',
    }) as any;

  describe('createBatchAccumulator', () => {
    it('flushes when batchSize reached', async () => {
      const flushed: any[] = [];
      const acc = svc.createBatchAccumulator({
        batch: true,
        batchSize: 2,
        batchTimeout: 5000,
      });
      acc.onFlush(async (msgs) => {
        flushed.push(...msgs);
      });
      await acc.add(mkMsg(1));
      await acc.add(mkMsg(2));
      expect(flushed).toHaveLength(2);
    });

    it('flushes on timeout', async () => {
      const flushed: any[] = [];
      const acc = svc.createBatchAccumulator({
        batch: true,
        batchSize: 100,
        batchTimeout: 50,
      });
      acc.onFlush(async (msgs) => {
        flushed.push(...msgs);
      });
      await acc.add(mkMsg(1));
      expect(flushed).toHaveLength(0);
      await new Promise((r) => setTimeout(r, 120));
      expect(flushed).toHaveLength(1);
    });
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
