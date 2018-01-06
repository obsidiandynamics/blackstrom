package com.obsidiandynamics.blackstrom.monitor.basic;

public final class BasicMonitorOptions {
  private String groupId = "monitor";
  
  private int gcIntervalMillis = 1_000;
  
  private int outcomeLifetimeMillis = 1_000;
  
  private int timeoutIntervalMillis = 1_000;
  
  String getGroupId() {
    return groupId;
  }

  public BasicMonitorOptions withGroupId(String groupId) {
    this.groupId = groupId;
    return this;
  }
  
  int getGCInterval() {
    return gcIntervalMillis;
  }

  public BasicMonitorOptions withGCInterval(int gcIntervalMillis) {
    this.gcIntervalMillis = gcIntervalMillis;
    return this;
  }

  int getOutcomeLifetime() {
    return outcomeLifetimeMillis;
  }

  public BasicMonitorOptions withOutcomeLifetime(int outcomeLifetimeMillis) {
    this.outcomeLifetimeMillis = outcomeLifetimeMillis;
    return this;
  }
  
  int getTimeoutInterval() {
    return timeoutIntervalMillis;
  }

  public BasicMonitorOptions withTimeoutInterval(int timeoutIntervalMillis) {
    this.timeoutIntervalMillis = timeoutIntervalMillis;
    return this;
  }
}
