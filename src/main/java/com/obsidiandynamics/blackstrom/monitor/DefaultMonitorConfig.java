package com.obsidiandynamics.blackstrom.monitor;

import com.obsidiandynamics.yconf.*;

@Y
public final class DefaultMonitorConfig {
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

  public DefaultMonitorConfig withGroupId(String groupId) {
    this.groupId = groupId;
    return this;
  }
  
  boolean isTrackingEnabled() {
    return trackingEnabled;
  }
  
  public DefaultMonitorConfig withTrackingEnabled(boolean trackingEnabled) {
    this.trackingEnabled = trackingEnabled;
    return this;
  }
  
  int getGCInterval() {
    return gcIntervalMillis;
  }

  public DefaultMonitorConfig withGCInterval(int gcIntervalMillis) {
    this.gcIntervalMillis = gcIntervalMillis;
    return this;
  }

  int getOutcomeLifetime() {
    return outcomeLifetimeMillis;
  }

  public DefaultMonitorConfig withOutcomeLifetime(int outcomeLifetimeMillis) {
    this.outcomeLifetimeMillis = outcomeLifetimeMillis;
    return this;
  }
  
  int getTimeoutInterval() {
    return timeoutIntervalMillis;
  }

  public DefaultMonitorConfig withTimeoutInterval(int timeoutIntervalMillis) {
    this.timeoutIntervalMillis = timeoutIntervalMillis;
    return this;
  }

  boolean isMetadataEnabled() {
    return metadataEnabled;
  }
  
  public DefaultMonitorConfig withMetadataEnabled(boolean metadataEnabled) {
    this.metadataEnabled = metadataEnabled;
    return this;
  }

  @Override
  public String toString() {
    return DefaultMonitorConfig.class.getSimpleName() + " [groupId=" + groupId + ", gcIntervalMillis=" + gcIntervalMillis
           + ", outcomeLifetimeMillis=" + outcomeLifetimeMillis + ", timeoutIntervalMillis=" + timeoutIntervalMillis
           + ", trackingEnabled=" + trackingEnabled + ", metadataEnabled=" + metadataEnabled + "]";
  }
  
  
}
