package com.obsidiandynamics.blackstrom.monitor;

import static com.obsidiandynamics.func.Functions.*;

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

  public void validate() {
    mustBeGreaterOrEqual(gcIntervalMillis, 1, illegalArgument("GC interval must be greater or equal to 1"));
    mustBeGreaterOrEqual(outcomeLifetimeMillis, 1, illegalArgument("Outcome lifetime must be greater or equal to 1"));
    mustBeGreaterOrEqual(timeoutIntervalMillis, 1, illegalArgument("Timeout interval must be greater or equal to 1"));
    mustExist(zlg, "Zlg cannot be null");
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
