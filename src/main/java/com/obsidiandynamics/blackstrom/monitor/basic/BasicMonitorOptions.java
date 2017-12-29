package com.obsidiandynamics.blackstrom.monitor.basic;

public final class BasicMonitorOptions {
  private int gcIntervalMillis = 1_000;
  
  private int decisionLifetimeMillis = 1_000;
  
  private int timeoutIntervalMillis = 1_000;

  public int getGCIntervalMillis() {
    return gcIntervalMillis;
  }

  public BasicMonitorOptions withGCIntervalMillis(int gcIntervalMillis) {
    this.gcIntervalMillis = gcIntervalMillis;
    return this;
  }

  public int getDecisionLifetimeMillis() {
    return decisionLifetimeMillis;
  }

  public BasicMonitorOptions withDecisionLifetimeMillis(int decisionLifetimeMillis) {
    this.decisionLifetimeMillis = decisionLifetimeMillis;
    return this;
  }
  
  public int getTimeoutIntervalMillis() {
    return timeoutIntervalMillis;
  }

  public BasicMonitorOptions withTimeoutIntervalMillis(int timeoutIntervalMillis) {
    this.timeoutIntervalMillis = timeoutIntervalMillis;
    return this;
  }
}
