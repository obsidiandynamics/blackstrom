package com.obsidiandynamics.blackstrom.monitor;

public final class DefaultMonitorOptions {
  private String groupId = "monitor";
  
  private int gcIntervalMillis = 1_000;
  
  private int outcomeLifetimeMillis = 1_000;
  
  private int timeoutIntervalMillis = 1_000;
  
  String getGroupId() {
    return groupId;
  }

  public DefaultMonitorOptions withGroupId(String groupId) {
    this.groupId = groupId;
    return this;
  }
  
  int getGCInterval() {
    return gcIntervalMillis;
  }

  public DefaultMonitorOptions withGCInterval(int gcIntervalMillis) {
    this.gcIntervalMillis = gcIntervalMillis;
    return this;
  }

  int getOutcomeLifetime() {
    return outcomeLifetimeMillis;
  }

  public DefaultMonitorOptions withOutcomeLifetime(int outcomeLifetimeMillis) {
    this.outcomeLifetimeMillis = outcomeLifetimeMillis;
    return this;
  }
  
  int getTimeoutInterval() {
    return timeoutIntervalMillis;
  }

  public DefaultMonitorOptions withTimeoutInterval(int timeoutIntervalMillis) {
    this.timeoutIntervalMillis = timeoutIntervalMillis;
    return this;
  }
}
