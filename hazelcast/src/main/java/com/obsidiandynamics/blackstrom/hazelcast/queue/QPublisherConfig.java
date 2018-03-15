package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.obsidiandynamics.yconf.*;

@Y
public final class QPublisherConfig {
  @YInject
  private StreamConfig streamConfig = new StreamConfig();

  StreamConfig getStreamConfig() {
    return streamConfig;
  }

  public QPublisherConfig withStreamConfig(StreamConfig streamConfig) {
    this.streamConfig = streamConfig;
    return this;
  }

  @Override
  public String toString() {
    return QPublisherConfig.class.getSimpleName() + " [streamConfig=" + streamConfig + "]";
  }
}
