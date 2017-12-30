package com.obsidiandynamics.blackstrom.monitor.basic;

public final class BasicMonitorOptions {
  private int gcIntervalMillis = 1_000;
  
  private int outcomeLifetimeMillis = 1_000;
  
  private int timeoutIntervalMillis = 1_000;

  public int getGCIntervalMillis() {
    return gcIntervalMillis;
  }

  public BasicMonitorOptions withGCIntervalMillis(int gcIntervalMillis) {
    this.gcIntervalMillis = gcIntervalMillis;
    return this;
  }

  public int getOutcomeLifetimeMillis() {
    return outcomeLifetimeMillis;
  }

  public BasicMonitorOptions withOutcomeLifetimeMillis(int outcomeLifetimeMillis) {
    this.outcomeLifetimeMillis = outcomeLifetimeMillis;
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
