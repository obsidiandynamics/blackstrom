package com.obsidiandynamics.blackstrom.spotter;

import static com.obsidiandynamics.func.Functions.*;

import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.zerolog.*;

@Y
public final class SpotterConfig {
  @YInject
  private long timeout = 10_000;
  
  @YInject
  private long gracePeriod = 5_000;
  
  @YInject
  private Zlg zlg = Zlg.forClass(Spotter.class).get();
  
  public void validate() {
    mustBeGreaterOrEqual(timeout, 0, illegalArgument("Timeout cannot be negative"));
    mustBeGreaterOrEqual(gracePeriod, 0, illegalArgument("Grace period cannot be negative"));
    mustExist(zlg, "Zlg cannot be null");
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeoutMillis) {
    this.timeout = mustBeGreaterOrEqual(timeoutMillis, 0, illegalArgument("Timeout cannot be negative"));
  }

  public SpotterConfig withTimeout(long timeoutMillis) {
    setTimeout(timeoutMillis);
    return this;
  }

  public long getGracePeriod() {
    return gracePeriod;
  }

  public void setGracePeriod(long gracePeriodMillis) {
    this.gracePeriod = mustBeGreaterOrEqual(gracePeriodMillis, 0, illegalArgument("Grace period cannot be negative"));
  }

  public SpotterConfig withGracePeriod(long gracePeriodMillis) {
    setGracePeriod(gracePeriodMillis);
    return this;
  }

  public Zlg getZlg() {
    return zlg;
  }

  public void setZlg(Zlg zlg) {
    this.zlg = mustExist(zlg, "Zlg cannot be null");
  }

  public SpotterConfig withZlg(Zlg zlg) {
    setZlg(zlg);
    return this;
  }

  @Override
  public String toString() {
    return SpotterConfig.class.getSimpleName() + " [timeout=" + timeout + ", gracePeriod=" + gracePeriod + ", zlg=" + zlg + "]";
  }
}
