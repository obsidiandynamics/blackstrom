package com.obsidiandynamics.blackstrom.monitor;

import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.zerolog.*;

@Y
public final class MonitorEngineConfig {
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
  
  @YInject
  private Zlg zlg = Zlg.forClass(MonitorEngine.class).get();
  
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
  
  Zlg getZlg() {
    return zlg;
  }
  
  public MonitorEngineConfig withZlg(Zlg zlg) {
    this.zlg = zlg;
    return this;
  }

  @Override
  public String toString() {
    return MonitorEngineConfig.class.getSimpleName() + " [gcIntervalMillis=" + gcIntervalMillis
           + ", outcomeLifetimeMillis=" + outcomeLifetimeMillis + ", timeoutIntervalMillis=" + timeoutIntervalMillis
           + ", trackingEnabled=" + trackingEnabled + ", metadataEnabled=" + metadataEnabled + "]";
  }
}
