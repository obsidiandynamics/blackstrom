package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.obsidiandynamics.yconf.*;

@Y
public final class PublisherConfig {
  @YInject
  private StreamConfig streamConfig = new StreamConfig();

  StreamConfig getStreamConfig() {
    return streamConfig;
  }

  public PublisherConfig withStreamConfig(StreamConfig streamConfig) {
    this.streamConfig = streamConfig;
    return this;
  }

  @Override
  public String toString() {
    return PublisherConfig.class.getSimpleName() + " [streamConfig=" + streamConfig + "]";
  }
}
