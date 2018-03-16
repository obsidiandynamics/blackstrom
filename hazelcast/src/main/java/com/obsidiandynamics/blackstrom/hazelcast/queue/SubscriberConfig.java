package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class SubscriberConfig {
  @YInject
  private StreamConfig streamConfig = new StreamConfig();
  
  @YInject
  private String group = null;
  
  @YInject
  private ElectionConfig electionConfig;

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
