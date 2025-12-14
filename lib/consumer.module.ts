import { Module, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import { ConsumerDiscoveryService } from './discovery/consumer-discovery.service';
import { ConsumerRegistryService } from './services/consumer-registry.service';

@Module({})
export class ConsumerModule implements OnModuleInit, OnApplicationShutdown {
  constructor(
    private readonly discoveryService: ConsumerDiscoveryService,
    private readonly registryService: ConsumerRegistryService,
  ) {}

  async onModuleInit(): Promise<void> {
    const consumers = this.discoveryService.discoverConsumers();

    this.registryService.registerConsumers(consumers);
    await this.registryService.startAll();
  }

  async onApplicationShutdown(): Promise<void> {
    await this.registryService.gracefulShutdown();
  }
}
