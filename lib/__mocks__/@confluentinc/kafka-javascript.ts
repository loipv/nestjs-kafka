/**
 * Shared manual mock of the Kafka client for unit specs (the native librdkafka
 * binding is absent under `npm ci --ignore-scripts`). Each spec still calls
 * `jest.mock('@confluentinc/kafka-javascript')` explicitly.
 *
 * `KafkaJS.Kafka` returns fresh producer / consumer / admin fakes; reach them
 * through `(KafkaJS.Kafka as jest.Mock).mock.results` and override per test
 * with `mockImplementation` / `mockImplementationOnce`.
 */
const resolved = () => jest.fn().mockResolvedValue(undefined);

const producer = () => ({
  connect: resolved(),
  disconnect: resolved(),
  send: resolved(),
});

const consumer = () => ({
  connect: resolved(),
  disconnect: resolved(),
  subscribe: resolved(),
  run: resolved(),
  commitOffsets: resolved(),
  pause: jest.fn(),
  resume: jest.fn(),
});

const admin = () => ({
  connect: resolved(),
  disconnect: resolved(),
  listTopics: jest.fn().mockResolvedValue([]),
  createTopics: resolved(),
  fetchOffsets: jest.fn().mockResolvedValue([]),
  fetchTopicOffsets: jest.fn().mockResolvedValue([]),
});

export const KafkaJS = {
  Kafka: jest.fn().mockImplementation(() => ({
    producer: jest.fn().mockImplementation(producer),
    consumer: jest.fn().mockImplementation(consumer),
    admin: jest.fn().mockImplementation(admin),
  })),
  logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 },
};
