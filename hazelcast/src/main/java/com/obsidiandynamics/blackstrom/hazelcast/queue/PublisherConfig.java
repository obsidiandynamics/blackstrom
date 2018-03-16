package com.obsidiandynamics.blackstrom.hazelcast.queue;

import org.slf4j.*;

import com.obsidiandynamics.yconf.*;

@Y
public final class PublisherConfig {
  private Logger log = LoggerFactory.getLogger(DefaultPublisher.class);
  
  @YInject
  private StreamConfig streamConfig = new StreamConfig();
  
  Logger getLog() {
    return log;
  }
  
  public PublisherConfig withLog(Logger log) {
    this.log = log;
    return this;
  }

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
