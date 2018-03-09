package com.obsidiandynamics.blackstrom.monitor;

import com.obsidiandynamics.yconf.*;

@Y
public final class MonitorEngineConfig {
  @YInject
  private String groupId = "monitor";
  
  @YInject
  private int gcIntervalMillis = 1_000;
  
  @YInject
  private int outcomeLifetimeMillis = 1_000;
  
  @YInject
  private int timeoutIntervalMillis = 1_000;
  
  @YInject
  private boolean trackingEnabled = true;
  
  @YInject
  private boolean metadataEnabled = false;
  
  String getGroupId() {
    return groupId;
  }

  public MonitorEngineConfig withGroupId(String groupId) {
    this.groupId = groupId;
    return this;
  }
  
  boolean isTrackingEnabled() {
    return trackingEnabled;
  }
  
  public MonitorEngineConfig withTrackingEnabled(boolean trackingEnabled) {
    this.trackingEnabled = trackingEnabled;
    return this;
  }
  
  int getGCInterval() {
    return gcIntervalMillis;
  }

  public MonitorEngineConfig withGCInterval(int gcIntervalMillis) {
    this.gcIntervalMillis = gcIntervalMillis;
    return this;
  }

  int getOutcomeLifetime() {
    return outcomeLifetimeMillis;
  }

  public MonitorEngineConfig withOutcomeLifetime(int outcomeLifetimeMillis) {
    this.outcomeLifetimeMillis = outcomeLifetimeMillis;
    return this;
  }
  
  int getTimeoutInterval() {
    return timeoutIntervalMillis;
  }

  public MonitorEngineConfig withTimeoutInterval(int timeoutIntervalMillis) {
    this.timeoutIntervalMillis = timeoutIntervalMillis;
    return this;
  }

  boolean isMetadataEnabled() {
    return metadataEnabled;
  }
  
  public MonitorEngineConfig withMetadataEnabled(boolean metadataEnabled) {
    this.metadataEnabled = metadataEnabled;
    return this;
  }

  @Override
  public String toString() {
    return MonitorEngineConfig.class.getSimpleName() + " [groupId=" + groupId + ", gcIntervalMillis=" + gcIntervalMillis
           + ", outcomeLifetimeMillis=" + outcomeLifetimeMillis + ", timeoutIntervalMillis=" + timeoutIntervalMillis
           + ", trackingEnabled=" + trackingEnabled + ", metadataEnabled=" + metadataEnabled + "]";
  }
  
  
}
