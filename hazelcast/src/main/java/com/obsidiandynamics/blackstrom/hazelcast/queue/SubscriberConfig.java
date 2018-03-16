package com.obsidiandynamics.blackstrom.hazelcast.queue;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class SubscriberConfig {
  private Logger log = LoggerFactory.getLogger(DefaultSubscriber.class);
  
  private ErrorHandler errorHandler = new LogAwareErrorHandler(this::getLog);
  
  @YInject
  private StreamConfig streamConfig = new StreamConfig();
  
  @YInject
  private String group = null;
  
  @YInject
  private ElectionConfig electionConfig;
  
  Logger getLog() {
    return log;
  }
  
  public SubscriberConfig withLog(Logger log) {
    this.log = log;
    return this;
  }
  
  ErrorHandler getErrorHandler() {
    return errorHandler;
  }
  
  public SubscriberConfig withErrorHandler(ErrorHandler errorHandler) {
    this.errorHandler = errorHandler;
    return this;
  }

  StreamConfig getStreamConfig() {
    return streamConfig;
  }

  public SubscriberConfig withStreamConfig(StreamConfig streamConfig) {
    this.streamConfig = streamConfig;
    return this;
  }
  
  boolean hasGroup() {
    return group != null;
  }

  String getGroup() {
    return group;
  }

  public SubscriberConfig withGroup(String group) {
    this.group = group;
    return this;
  }

  ElectionConfig getElectionConfig() {
    return electionConfig;
  }

  public SubscriberConfig withElectionConfig(ElectionConfig electionConfig) {
    this.electionConfig = electionConfig;
    return this;
  }
}
