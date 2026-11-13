/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  Injectable,
  Inject,
  Logger,
  OnModuleDestroy,
  Optional,
} from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaCoreService } from './kafka-core.service';

type Consumer = KafkaJS.Consumer;
type EachBatchPayload = KafkaJS.EachBatchPayload;
type EachMessagePayload = KafkaJS.EachMessagePayload;
import { BatchProcessorService } from './batch-processor.service';
import { IdempotencyService } from './idempotency.service';
import { DlqService } from './dlq.service';
import { TracingService } from './tracing.service';
import { PartitionRetryTracker } from './partition-retry-tracker';
import {
  ConsumerMetadata,
  ConsumerModuleOptions,
  ConsumerOptions,
  CONSUMER_MODULE_OPTIONS,
  DEFAULT_KAFKA_CONNECTION,
  deserializeMessage,
  RetryVerdict,
  MAX_RETRY_DELAY_MS,
} from '../interfaces';

interface TopicHandler {
  metadata: ConsumerMetadata;
  handler: (...args: any[]) => Promise<void>;
}

/** librdkafka ERR__REVOKE_PARTITIONS (a literal: unit tests mock the module) */
const ERR_REVOKE_PARTITIONS = -174;
/** librdkafka ERR__NO_OFFSET: a commit with nothing stored to commit */
const ERR_NO_OFFSET = -168;
/** Upper bound for one synchronous commit (autoCommit: false) */
const COMMIT_TIMEOUT_MS = 10000;
/** Default chunk size, shared with BatchProcessorService.splitIntoBatches */
const DEFAULT_BATCH_SIZE = 100;
/** Consumer options that ConsumerModule.forRoot() may default */
const DEFAULTABLE_KEYS = [
  'partitionAssigners',
  'allowAutoTopicCreation',
  'sessionTimeout',
  'heartbeatInterval',
  'rebalanceTimeout',
  'autoCommit',
  'autoCommitInterval',
  'fromBeginning',
  'autoCreateTopicPartitions',
  'autoCreateTopicReplicationFactor',
] as const satisfies readonly (keyof ConsumerOptions &
  keyof ConsumerModuleOptions)[];

/** Largest delay setTimeout honours; Node turns anything above it into 1 ms */
const MAX_TIMER_MS = 2147483647;

interface ConsumerGroup {
  groupId: string;
  /** `<connection>:<groupId>` */
  groupKey: string;
  connection: string;
  /** Created in startConsumerGroup, once every topic of the group is registered */
  consumer?: Consumer;
  topics: Map<string, TopicHandler>;
  options: ConsumerOptions; // Use first consumer's options for shared settings
  isRunning: boolean;
  /** Set by the group's first consumer; every consumer in a group must match */
  isBatch: boolean;
}

@Injectable()
export class ConsumerRegistryService implements OnModuleDestroy {
  private readonly logger = new Logger(ConsumerRegistryService.name);
  private consumerGroups = new Map<string, ConsumerGroup>();
  private isShuttingDown = false;
  private shutdownPromise?: Promise<void>;
  private startPromise?: Promise<void>;
  /** Set by registerConsumers(): startAll() before discovery is an error */
  private registered = false;
  private readonly retryTracker = new PartitionRetryTracker();

  constructor(
    private readonly kafkaCore: KafkaCoreService,
    private readonly batchProcessor: BatchProcessorService,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqService: DlqService,
    @Optional() private readonly tracingService?: TracingService,
    @Optional()
    @Inject(CONSUMER_MODULE_OPTIONS)
    private readonly moduleOptions?: ConsumerModuleOptions,
  ) {}

  /**
   * Merge module default options with decorator options.
   * Decorator options take precedence over module defaults.
   */
  private mergeWithDefaults(options: ConsumerOptions): ConsumerOptions {
    if (!this.moduleOptions) {
      return options;
    }

    const defaults = this.moduleOptions;

    // Decorator values win; an undefined decorator value never hides a default
    const merged: ConsumerOptions = { ...options };
    for (const key of DEFAULTABLE_KEYS) {
      if (merged[key] === undefined && defaults[key] !== undefined) {
        (merged as Record<string, unknown>)[key] = defaults[key];
      }
    }

    // Deep-merge retry per field (decorator wins per field) — must happen
    // after the spread above, which would otherwise clobber with options.retry
    if (defaults.retry || options.retry) {
      const retry: Record<string, unknown> = { ...defaults.retry };
      for (const [key, value] of Object.entries(options.retry ?? {})) {
        if (value !== undefined) retry[key] = value;
      }
      merged.retry = retry as ConsumerOptions['retry'];
    }

    return merged;
  }

  registerConsumers(consumers: ConsumerMetadata[]): void {
    this.registered = true;
    // Group consumers by groupId
    for (const metadata of consumers) {
      this.registerConsumer(metadata);
    }
  }

  private registerConsumer(metadata: ConsumerMetadata): void {
    const { topic, connection, target, methodName } = metadata;
    // Merge module defaults with decorator options
    const options = this.mergeWithDefaults(metadata.options);
    const groupId = options.groupId || `${topic}-group`;
    const connectionName = connection || DEFAULT_KAFKA_CONNECTION;

    this.validateTimerOptions(topic, options);

    // Fail fast on a mistyped DLQ connection instead of at the first failure
    if (options.dlq?.connection) {
      this.kafkaCore.getKafka(options.dlq.connection);
    }

    // Key includes both connection and groupId to support same groupId on different connections
    const groupKey = `${connectionName}:${groupId}`;

    // Get or create consumer group
    let group = this.consumerGroups.get(groupKey);

    if (!group) {
      group = {
        groupId,
        groupKey,
        connection: connectionName,
        topics: new Map(),
        options,
        isRunning: false,
        isBatch: !!options.batch,
      };

      this.consumerGroups.set(groupKey, group);

      this.logger.log(
        `Created consumer group: ${groupId} (connection: ${connectionName})`,
      );
    }

    if (group.isBatch !== !!options.batch) {
      throw new Error(
        `Consumer group "${groupId}" mixes batch and non-batch consumers; use separate groupIds`,
      );
    }

    if (group.topics.has(topic)) {
      throw new Error(
        `Consumer group "${groupId}" already has a handler for topic "${topic}"`,
      );
    }

    // Add topic handler to the group with merged options
    const handler = target[methodName].bind(target);
    const mergedMetadata: ConsumerMetadata = {
      ...metadata,
      options, // Use merged options
    };
    group.topics.set(topic, { metadata: mergedMetadata, handler });

    this.logger.log(
      `Registered topic "${topic}" in group "${groupId}" (connection: ${connectionName})`,
    );
  }

  /**
   * Timer options must be positive and within setTimeout's range; backoff
   * multipliers must be finite and >= 1 (a smaller one shrinks the delay
   * towards a tight redelivery loop).
   */
  private validateTimerOptions(topic: string, options: ConsumerOptions): void {
    const multipliers: Array<[string, number | undefined]> = [
      ['retry.multiplier', options.retry?.multiplier],
      ['dlq.retryBackoffMultiplier', options.dlq?.retryBackoffMultiplier],
    ];
    for (const [name, value] of multipliers) {
      if (value !== undefined && !(Number.isFinite(value) && value >= 1)) {
        throw new Error(
          `Consumer "${topic}": ${name} must be a finite number >= 1`,
        );
      }
    }

    const timers: Array<[string, number | undefined]> = [
      ['retry.maxRetryTime', options.retry?.maxRetryTime],
      ['retry.initialRetryTime', options.retry?.initialRetryTime],
      ['dlq.sendTimeout', options.dlq?.sendTimeout],
      ['dlq.retryDelay', options.dlq?.retryDelay],
    ];
    for (const [name, value] of timers) {
      if (value === undefined) continue;
      if (!(Number.isFinite(value) && value > 0)) {
        throw new Error(
          `Consumer "${topic}": ${name} must be a positive number`,
        );
      }
      if (value > MAX_TIMER_MS) {
        throw new Error(
          `Consumer "${topic}": ${name} must be at most ${MAX_TIMER_MS} ms`,
        );
      }
    }
  }

  /**
   * Start every registered group. Called by ConsumerModule at bootstrap, or by
   * the application with `ConsumerModule.forRoot({ autoStart: false })`.
   * Concurrent and repeated calls share one start. Waits for every group to
   * settle; on failure closes the started groups and the producers
   * (abortStartup) before rejecting with the first error.
   */
  async startAll(): Promise<void> {
    if (!this.registered) {
      throw new Error(
        'startAll() called before the application was initialised (call app.init() or app.listen() first)',
      );
    }
    this.startPromise ??= this.start();
    return this.startPromise;
  }

  /** Start every group once; on failure close what started, then reject. */
  private async start(): Promise<void> {
    if (this.isShuttingDown) throw new Error('Consumers are shutting down');
    const results = await Promise.allSettled(
      Array.from(this.consumerGroups.values()).map((group) =>
        this.startConsumerGroup(group),
      ),
    );
    const failed = results.find(
      (r): r is PromiseRejectedResult => r.status === 'rejected',
    );
    if (failed) {
      await this.abortStartup().catch((error: unknown) =>
        this.logger.error('Cleanup after a failed start failed', error),
      );
      throw failed.reason;
    }
  }

  /**
   * Startup failed: close the consumers that did start and every producer.
   * Nest skips shutdown hooks when initialisation fails, so without this the
   * open clients keep the process alive.
   */
  async abortStartup(): Promise<void> {
    await this.gracefulShutdown();
    await this.kafkaCore.disconnectAll();
  }

  /**
   * Create missing topics on one connection (sizing from the owning consumer's
   * options), then wait for the subscribed ones to become visible. Failures are
   * logged, not thrown — subscribing still proceeds.
   */
  private async autoCreateTopics(
    connection: string,
    topicsToCreate: Map<string, ConsumerOptions>,
    isSubscribed: (topic: string) => boolean,
  ): Promise<void> {
    const admin = this.kafkaCore.getKafka(connection).admin();
    try {
      await admin.connect();
      const existingTopics = await admin.listTopics();

      // Create topics ONE BY ONE: when multiple consumer groups start
      // concurrently (e.g. a topic and its DLQ spy), they can share a
      // topic in their creation lists — a single atomic createTopics call
      // would fail wholesale on "already exists" and skip the rest.
      for (const [topic, opts] of topicsToCreate) {
        if (existingTopics.includes(topic)) continue;
        try {
          await admin.createTopics({
            topics: [
              {
                topic,
                numPartitions: opts.autoCreateTopicPartitions ?? 1,
                replicationFactor: opts.autoCreateTopicReplicationFactor ?? 1,
              },
            ],
          });
          this.logger.log(`Auto-created topic: ${topic} (${connection})`);
        } catch (error) {
          // Concurrent creation by another group is fine — verify it exists
          if (!(await admin.listTopics()).includes(topic)) {
            throw error;
          }
        }
      }

      // Wait for topic metadata to propagate before subscribing —
      // subscribing to a topic whose metadata isn't visible yet leaves
      // the consumer with zero assignments (silent no-consumption).
      const waitFor = [...topicsToCreate.keys()].filter(isSubscribed);
      const deadline = Date.now() + 10_000;
      let visible = new Set(await admin.listTopics());
      while (waitFor.some((t) => !visible.has(t)) && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 200));
        visible = new Set(await admin.listTopics());
      }
    } catch (error) {
      this.logger.warn(
        `Failed to auto-create topics (${connection}): ${error}`,
      );
    } finally {
      await admin.disconnect();
    }
  }

  /**
   * Build the group's consumer: kafkaJS options, a rebalance callback that
   * drops retry state of revoked partitions, and (batch groups) a client batch
   * size large enough for the biggest `batchSize` in the group.
   */
  private createConsumer(group: ConsumerGroup): Consumer {
    const config: KafkaJS.ConsumerConstructorConfig = {
      kafkaJS: this.buildConsumerConfig(group.groupId, group.options),
      rebalance_cb: (
        err: { code: number },
        assignment: Array<{ topic: string; partition: number }>,
      ) => this.onRebalance(group, err, assignment),
    };
    if (group.isBatch) {
      config['js.consumer.max.batch.size'] = Math.max(
        ...[...group.topics.values()].map(({ metadata }) => {
          const size = metadata.options.batchSize;
          return size !== undefined && Number.isInteger(size) && size > 0
            ? size
            : DEFAULT_BATCH_SIZE;
        }),
      );
    }
    return this.kafkaCore.getKafka(group.connection).consumer(config);
  }

  /**
   * On revoke: forget retry state and resume the revoked partitions. Runs
   * before the client unassigns them, so the resume is applied while they are
   * still assigned; the client keeps paused partitions across rebalances and
   * would otherwise leave a re-assigned partition paused.
   */
  private onRebalance(
    group: ConsumerGroup,
    err: { code: number },
    assignment: Array<{ topic: string; partition: number }>,
  ): void {
    if (err.code !== ERR_REVOKE_PARTITIONS || assignment.length === 0) return;

    const partitionKeys = assignment.map(({ topic, partition }) =>
      PartitionRetryTracker.partitionKey(group.groupKey, topic, partition),
    );
    this.retryTracker.revoke(partitionKeys);

    const byTopic = new Map<string, number[]>();
    for (const { topic, partition } of assignment) {
      byTopic.set(topic, [...(byTopic.get(topic) ?? []), partition]);
    }
    try {
      group.consumer?.resume(
        [...byTopic].map(([topic, partitions]) => ({ topic, partitions })),
      );
    } catch (error) {
      this.logger.debug(
        `Resume of revoked partitions in "${group.groupId}" failed: ${String(error)}`,
      );
    }
  }

  private async startConsumerGroup(group: ConsumerGroup): Promise<void> {
    const { groupId, topics, connection } = group;
    const consumer = this.createConsumer(group);
    group.consumer = consumer;
    this.kafkaCore.setConsumerStatus(group.groupKey, 'starting');

    try {
      await consumer.connect();
      this.throwIfShuttingDown();

      // Subscribe to all topics in this group
      const topicList = Array.from(topics.keys());

      // Topics to auto-create, grouped by connection — DLQ topics may live on
      // a different cluster (dlq.connection) than the consumer itself
      const topicsToCreate = new Map<string, Map<string, ConsumerOptions>>();
      const addTopic = (conn: string, t: string, opts: ConsumerOptions) => {
        if (!topicsToCreate.has(conn)) topicsToCreate.set(conn, new Map());
        topicsToCreate.get(conn)!.set(t, opts);
      };
      for (const topic of topicList) {
        const opts = topics.get(topic)!.metadata.options;
        if (!opts.allowAutoTopicCreation) continue;

        addTopic(connection, topic, opts);
        const dlqConnection = opts.dlq?.connection || connection;
        if (opts.dlq?.topic) {
          addTopic(dlqConnection, opts.dlq.topic, opts);
        }
      }

      for (const [conn, connTopics] of topicsToCreate) {
        await this.autoCreateTopics(conn, connTopics, (t) => topics.has(t));
      }

      for (const topic of topicList) {
        await consumer.subscribe({ topic });
        // Note: fromBeginning is configured at consumer level in confluent-kafka-javascript
      }
      this.throwIfShuttingDown();

      this.logger.log(
        `Consumer group "${groupId}" subscribed to topics: ${topicList.join(', ')}`,
      );

      // Every consumer in a group is batch or every one is not (enforced at registration)
      if (group.isBatch) {
        await this.startBatchGroupConsumer(group);
      } else {
        await this.startMessageGroupConsumer(group);
      }
      this.throwIfShuttingDown();

      group.isRunning = true;
      this.kafkaCore.setConsumerStatus(group.groupKey, 'running');
      this.logger.log(`Started consumer group: ${groupId}`);
    } catch (error) {
      if (this.isShuttingDown) {
        this.logger.warn(
          `Start of consumer group "${groupId}" stopped by shutdown`,
        );
        this.kafkaCore.setConsumerStatus(group.groupKey, 'stopped');
      } else {
        this.logger.error(`Failed to start consumer group: ${groupId}`, error);
        this.kafkaCore.setConsumerStatus(group.groupKey, 'failed');
      }
      try {
        await consumer.disconnect();
      } catch (disconnectError) {
        this.logger.error(
          `Failed to disconnect consumer group after a failed start: ${groupId}`,
          disconnectError,
        );
      }
      // Already disconnected: shutdown() must not touch it (or its status) again
      group.consumer = undefined;
      throw error;
    }
  }

  /** A start racing a shutdown stops at its next step (no orphan consumer). */
  private throwIfShuttingDown(): void {
    if (this.isShuttingDown) throw new Error('Consumers are shutting down');
  }

  /** Run config shared by message and batch groups. */
  private baseRunConfig(options: ConsumerOptions): KafkaJS.ConsumerRunConfig {
    // Note: restartOnFailure is not supported in confluent-kafka-javascript
    const runConfig: KafkaJS.ConsumerRunConfig = {};
    if (
      options.partitionsConsumedConcurrently !== undefined &&
      options.partitionsConsumedConcurrently > 0
    ) {
      runConfig.partitionsConsumedConcurrently =
        options.partitionsConsumedConcurrently;
    }
    return runConfig;
  }

  private async startMessageGroupConsumer(group: ConsumerGroup): Promise<void> {
    const consumer = group.consumer!;
    const runConfig = this.baseRunConfig(group.options);

    runConfig.eachMessage = async (payload: EachMessagePayload) => {
      // Never return here: a normal return stores the offset, which the client
      // commits on close — the message would be lost.
      if (this.isShuttingDown) throw new Error('Consumer is shutting down');

      // Throws (after pausing the partition) when the message must be retried
      await this.processMessage(group, payload);

      // With auto-commit off the client never commits on its own
      if (group.options.autoCommit === false) {
        const { topic, partition, message } = payload;
        await this.commitMessageOffset(
          consumer,
          topic,
          partition,
          message.offset,
        );
      }
    };

    // Resolves once the client has started its fetch loop. Handler and broker
    // errors never reject it: the client logs them and redelivers the message.
    await consumer.run(runConfig);
  }

  /**
   * One delivery of one message. Resolves when the message may be acked
   * (handled, duplicate, no handler, dead-lettered or skipped); throws after
   * pausing the partition when it must be retried.
   */
  private async processMessage(
    group: ConsumerGroup,
    payload: EachMessagePayload,
  ): Promise<void> {
    const { groupId, groupKey, topics, connection } = group;
    const { topic, message, partition } = payload;

    const topicHandler = topics.get(topic);
    if (!topicHandler) {
      this.logger.warn(`No handler found for topic: ${topic}`);
      return;
    }

    const { metadata, handler } = topicHandler;
    const topicOptions = metadata.options;
    const namespace = this.idempotencyNamespace(connection, groupId, topic);

    const processOnce = async () => {
      const processedMessage =
        topicOptions.deserialize !== false
          ? deserializeMessage(message, topic, partition)
          : message;
      await handler(processedMessage);
    };

    // One trace span per delivery (each retry is a redelivery)
    const invokeOnce = async () => {
      if (this.tracingService?.isEnabled()) {
        return this.tracingService.withConsumeSpan(
          {
            topic,
            partition,
            offset: message.offset,
            key: message.key?.toString(),
            groupId,
            headers: message.headers,
          },
          processOnce,
        );
      }
      return processOnce();
    };

    const attemptKey = PartitionRetryTracker.attemptKey(
      groupKey,
      topic,
      partition,
      message.offset,
    );
    // Everything that can throw (the idempotency extractor included) runs
    // inside the try, so every failure pauses the partition first.
    try {
      if (
        topicOptions.idempotencyKey &&
        this.idempotencyService.isProcessed(
          message,
          topicOptions.idempotencyKey,
          namespace,
        )
      ) {
        this.logger.debug(`Skipping duplicate message from ${topic}`);
        return;
      }
      await invokeOnce();
      if (topicOptions.idempotencyKey) {
        this.idempotencyService.markProcessed(
          message,
          topicOptions.idempotencyKey,
          namespace,
          topicOptions.idempotencyTtl,
        );
      }
    } catch (error) {
      await this.handleHandlerFailure({
        group,
        metadata,
        partition,
        messages: [message],
        attemptKey,
        error,
        pause: () => payload.pause(),
      });
      return;
    }

    this.retryTracker.clear(attemptKey);
  }

  /**
   * Commit `offset + 1` (auto-commit off). A failure is only logged: the
   * offset stays stored and the next successful commit covers it; rethrowing
   * would redeliver a message that was handled.
   */
  private async commitMessageOffset(
    consumer: Consumer,
    topic: string,
    partition: number,
    offset: string,
  ): Promise<void> {
    const next = (BigInt(offset) + 1n).toString();
    await this.boundedCommit(
      () => consumer.commitOffsets([{ topic, partition, offset: next }]),
      topic,
      partition,
      next,
    );
  }

  /**
   * Run one commit, waiting at most COMMIT_TIMEOUT_MS: a commit can hang while
   * the group coordinator is unavailable, and app.close() waits for workers.
   * Never rejects; a failure (other than ERR__NO_OFFSET) or a timeout is
   * logged, and a commit settling later is ignored.
   */
  private async boundedCommit(
    commit: () => Promise<void>,
    topic: string,
    partition: number,
    offset: string,
  ): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    const outcome = await Promise.race([
      commit().then(
        () => ({ ok: true as const }),
        (error: unknown) => ({ ok: false as const, error }),
      ),
      new Promise<'timeout'>((resolve) => {
        timer = setTimeout(() => resolve('timeout'), COMMIT_TIMEOUT_MS);
        timer.unref();
      }),
    ]);
    clearTimeout(timer);
    if (outcome === 'timeout') {
      this.logger.warn(`Commit timed out for ${topic}[${partition}]@${offset}`);
    } else if (
      !outcome.ok &&
      // Nothing new stored (another partition's commit covered it)
      (outcome.error as { code?: number } | null)?.code !== ERR_NO_OFFSET
    ) {
      this.logCommitFailure(topic, partition, offset, outcome.error);
    }
  }

  private logCommitFailure(
    topic: string,
    partition: number,
    offset: string,
    error: unknown,
  ): void {
    this.logger.warn(
      `Commit failed for ${topic}[${partition}]@${offset}: ${String(error)}`,
    );
  }

  private async startBatchGroupConsumer(group: ConsumerGroup): Promise<void> {
    const { topics, options } = group;
    const consumer = group.consumer!;
    const runConfig = this.baseRunConfig(options);

    // Batch groups commit ONLY what we explicitly resolve — never auto-resolve
    // the whole fetched batch. On a throw or an early return the client seeks
    // back to the first unresolved offset.
    runConfig.eachBatchAutoResolve = false;

    runConfig.eachBatch = async (payload: EachBatchPayload) => {
      if (this.isShuttingDown) return; // nothing resolved → redelivered

      const { batch } = payload;
      const { topic, messages } = batch;

      const topicHandler = topics.get(topic);
      if (!topicHandler) {
        this.logger.warn(`No handler found for topic: ${topic}`);
        return;
      }

      for (const chunk of this.batchProcessor.splitIntoBatches(
        messages,
        topicHandler.metadata.options.batchSize,
      )) {
        // The client never resets isRunning() during disconnect: check our own
        // flag too so shutdown does not wait for every remaining chunk.
        if (this.isShuttingDown || !payload.isRunning() || payload.isStale()) {
          return;
        }

        // Throws (after pausing the partition) when the chunk must be retried
        await this.processChunk(group, topicHandler, payload, chunk);
        const lastOffset = chunk[chunk.length - 1].offset;
        payload.resolveOffset(lastOffset);

        // With auto-commit off the client never commits on its own
        if (options.autoCommit === false) {
          await this.boundedCommit(
            () => payload.commitOffsetsIfNecessary(),
            topic,
            payload.batch.partition,
            (BigInt(lastOffset) + 1n).toString(),
          );
        }
      }
    };

    // Resolves once the client has started its fetch loop. Handler and broker
    // errors never reject it: the client logs them and redelivers the message.
    await consumer.run(runConfig);
  }

  /**
   * Hand one chunk to the batch handler. Resolves when the chunk may be
   * resolved (handled, all duplicates, dead-lettered or skipped); throws after
   * pausing the partition when it must be retried.
   */
  private async processChunk(
    group: ConsumerGroup,
    { metadata, handler }: TopicHandler,
    payload: EachBatchPayload,
    chunk: KafkaJS.KafkaMessage[],
  ): Promise<void> {
    const { topic, partition } = payload.batch;
    const options = metadata.options;
    const namespace = this.idempotencyNamespace(
      group.connection,
      group.groupId,
      topic,
    );

    const attemptKey = PartitionRetryTracker.attemptKey(
      group.groupKey,
      topic,
      partition,
      chunk[0].offset,
    );

    // Everything that can throw (the idempotency extractor included) runs
    // inside the try, so every failure pauses the partition first.
    let processable = chunk;
    try {
      if (options.idempotencyKey) {
        processable = this.idempotencyService.filterDuplicates(
          chunk,
          options.idempotencyKey,
          namespace,
        );
      }
      if (processable.length === 0) return;
      await this.invokeBatchHandler(
        group,
        handler,
        options,
        topic,
        partition,
        processable,
      );
      if (options.idempotencyKey) {
        for (const msg of processable) {
          this.idempotencyService.markProcessed(
            msg,
            options.idempotencyKey,
            namespace,
            options.idempotencyTtl,
          );
        }
      }
    } catch (error) {
      await this.handleHandlerFailure({
        group,
        metadata,
        partition,
        messages: processable,
        attemptKey,
        error,
        pause: () => payload.pause(),
      });
      return;
    }

    this.retryTracker.clear(attemptKey);
  }

  /** Deserialize, optionally group by key, and call the batch handler (traced). */
  private async invokeBatchHandler(
    group: ConsumerGroup,
    handler: TopicHandler['handler'],
    options: ConsumerOptions,
    topic: string,
    partition: number,
    messages: KafkaJS.KafkaMessage[],
  ): Promise<void> {
    const deserialized =
      options.deserialize !== false
        ? messages.map((msg) => deserializeMessage(msg, topic, partition))
        : messages;

    const processBatch = async () => {
      if (options.groupByKey) {
        await handler(this.batchProcessor.groupMessagesByKey(deserialized));
      } else {
        await handler(deserialized);
      }
    };

    // First message becomes the span parent, the others are linked
    if (this.tracingService?.isEnabled()) {
      return this.tracingService.withBatchConsumeSpan(
        {
          topic,
          partition,
          groupId: group.groupId,
          messagesHeaders: messages.map((msg) => ({
            offset: msg.offset,
            key: msg.key?.toString(),
            headers: msg.headers,
          })),
        },
        processBatch,
      );
    }
    return processBatch();
  }

  /**
   * One failed delivery of a message or chunk. Resolves on a terminal verdict
   * (dead-lettered or skipped: the caller acks). Otherwise pauses the payload's
   * partition, schedules its resume after the backoff delay and rethrows the
   * handler error, so the client seeks back and redelivers after the resume.
   * Never sleeps: the consumer keeps polling and stays in its group.
   */
  private async handleHandlerFailure(ctx: {
    group: ConsumerGroup;
    metadata: ConsumerMetadata;
    partition: number;
    messages: KafkaJS.KafkaMessage[];
    attemptKey: string;
    error: unknown;
    pause: () => (() => void) | undefined;
  }): Promise<void> {
    const { group, metadata, partition, messages, attemptKey } = ctx;
    // The client logs `${e}: ${e.stack}` before seeking back; a null or
    // undefined rejection would throw there and skip the seek (message lost).
    const error =
      ctx.error instanceof Error
        ? ctx.error
        : new Error(
            `Handler rejected with a non-Error value: ${String(ctx.error)}`,
          );
    const { topic, options } = metadata;
    const attempt = this.retryTracker.nextAttempt(attemptKey);

    this.logger.error(
      `Error processing ${messages.length > 1 ? `${messages.length} messages` : 'message'} ` +
        `from ${topic} [${partition}] (attempt ${attempt})`,
      error,
    );

    const verdict = options.dlq
      ? await this.dlqService.handleFailure(
          messages,
          error,
          options.dlq,
          topic,
          partition,
          group.connection,
          attempt,
          this.maxRetryDelay(options.retry),
          this.retryTracker.deadLettered(attemptKey),
        ) // never rejects: a failed DLQ send returns a retry verdict
      : this.evaluateRetry(attempt, options);

    if (verdict.action === 'complete') {
      this.retryTracker.clear(attemptKey);
      this.logger.error(
        `Message(s) from ${topic} [${partition}] completed without success after ` +
          `${attempt} attempts (${options.dlq ? 'dead-lettered' : 'skipped'}). ` +
          `First offset: ${messages[0]?.offset ?? 'n/a'}`,
      );
      return;
    }

    let resume: (() => void) | undefined;
    try {
      resume = ctx.pause();
    } catch (pauseError) {
      // The consumer is disconnecting: the throw below still prevents the ack
      this.logger.debug(`Pause failed: ${String(pauseError)}`);
    }
    if (resume) {
      this.retryTracker.scheduleResume(
        PartitionRetryTracker.partitionKey(group.groupKey, topic, partition),
        verdict.delayMs,
        resume,
      );
    }
    throw error;
  }

  /** Idempotency keys are scoped to one consumer: `<connection>:<groupId>|<topic>`. */
  private idempotencyNamespace(
    connection: string,
    groupId: string,
    topic: string,
  ): string {
    return `${connection}:${groupId}|${topic}`;
  }

  /** Build the confluent consumer config with only defined values. */
  private buildConsumerConfig(
    groupId: string,
    options: ConsumerOptions,
  ): KafkaJS.ConsumerConfig {
    const consumerConfig: KafkaJS.ConsumerConfig = {
      groupId,
      maxBytesPerPartition: 1048576,
      autoCommit: options.autoCommit !== false,
    };

    // Add optional settings only if defined
    if (options.sessionTimeout !== undefined) {
      consumerConfig.sessionTimeout = options.sessionTimeout;
    }
    if (options.heartbeatInterval !== undefined) {
      consumerConfig.heartbeatInterval = options.heartbeatInterval;
    }
    if (options.rebalanceTimeout !== undefined) {
      consumerConfig.rebalanceTimeout = options.rebalanceTimeout;
    }
    if (options.fromBeginning !== undefined) {
      consumerConfig.fromBeginning = options.fromBeginning;
    }
    if (options.autoCommitInterval !== undefined) {
      consumerConfig.autoCommitInterval = options.autoCommitInterval;
    }
    if (options.allowAutoTopicCreation !== undefined) {
      consumerConfig.allowAutoTopicCreation = options.allowAutoTopicCreation;
    }
    if (options.partitionAssigners && options.partitionAssigners.length > 0) {
      consumerConfig.partitionAssigners =
        options.partitionAssigners as KafkaJS.PartitionAssigners[];
    }
    // `options.retry` configures handler retries only. It is NOT forwarded to
    // the client's request backoff (retry.backoff.ms is limited to 300000).

    return consumerConfig;
  }

  /** Effective cap for every retry delay: `retry.maxRetryTime`, default 30s. */
  private maxRetryDelay(retry?: ConsumerOptions['retry']): number {
    return retry?.maxRetryTime ?? MAX_RETRY_DELAY_MS;
  }

  private computeRetryDelay(
    attempt: number,
    retry?: ConsumerOptions['retry'],
  ): number {
    const baseDelay = retry?.initialRetryTime ?? 1000;
    const multiplier = retry?.multiplier ?? 2;
    return Math.min(
      baseDelay * multiplier ** (attempt - 1),
      this.maxRetryDelay(retry),
    );
  }

  /** Verdict for the non-DLQ path (pure). */
  private evaluateRetry(
    attempt: number,
    options: ConsumerOptions,
  ): RetryVerdict {
    const maxRetries = options.retry?.retries ?? 3;
    if (
      attempt <= maxRetries ||
      !(options.retry?.skipMessageOnMaxRetries ?? false)
    ) {
      // skip=false (default) → retry indefinitely: handler errors never crash
      // the consumer and never lose the message. skip=true → bounded by maxRetries.
      return {
        action: 'retry',
        delayMs: this.computeRetryDelay(attempt, options.retry),
      };
    }
    return { action: 'complete' };
  }

  /** Idempotent: every call returns the first call's promise. */
  gracefulShutdown(): Promise<void> {
    this.shutdownPromise ??= this.shutdown();
    return this.shutdownPromise;
  }

  private async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    // Pending resumes are dropped; paused partitions are redelivered next boot
    this.retryTracker.close();

    this.logger.log('Starting graceful shutdown of consumers...');

    const shutdownPromises = Array.from(this.consumerGroups.values()).map(
      async (group) => {
        try {
          // Also a group still starting: startConsumerGroup stops at its
          // next step, and this disconnect covers a connect already done.
          if (group.consumer) {
            // Note: stop() is not supported in confluent-kafka-javascript, use disconnect() directly
            await group.consumer.disconnect();
            this.kafkaCore.setConsumerStatus(group.groupKey, 'stopped');
          }
        } catch (error) {
          this.logger.error(
            `Error during consumer group shutdown: ${group.groupId}`,
            error,
          );
        }
      },
    );

    await Promise.all(shutdownPromises);
    this.idempotencyService.stopCleanup();

    this.logger.log('All consumer groups shut down gracefully');
  }

  /**
   * Stop consumers in the earliest shutdown hook: every onModuleDestroy runs
   * before any onApplicationShutdown, so consumers always stop before
   * KafkaClient disconnects the producers, whatever the import order.
   */
  async onModuleDestroy(): Promise<void> {
    await this.gracefulShutdown();
  }
}
