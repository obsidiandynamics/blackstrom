package com.obsidiandynamics.blackstrom.hazelcast.elect;

import com.obsidiandynamics.yconf.*;

@Y
public final class ElectionConfig {
  @YInject
  private int scavengeIntervalMillis = 100;
  
  @YInject
  private int leaseDurationMillis = 60_000;
  
  private ScavengeWatcher scavengeWatcher = ScavengeWatcher.nop();
  
  int getScavengeInterval() {
    return scavengeIntervalMillis;
  }

  public ElectionConfig withScavengeInterval(int scavengeIntervalMillis) {
    this.scavengeIntervalMillis = scavengeIntervalMillis;
    return this;
  }

  int getLeaseDuration() {
    return leaseDurationMillis;
  }

  public ElectionConfig withLeaseDuration(int leaseDurationMillis) {
    this.leaseDurationMillis = leaseDurationMillis;
    return this;
  }
  
  ScavengeWatcher getScavengeWatcher() {
    return scavengeWatcher;
  }

  ElectionConfig withScavengeWatcher(ScavengeWatcher scavengeWatcher) {
    this.scavengeWatcher = scavengeWatcher;
    return this;
  }

  @Override
  public String toString() {
    return ElectionConfig.class.getSimpleName() + " [scavengeIntervalMillis=" + scavengeIntervalMillis + 
        ", leaseDurationMillis=" + leaseDurationMillis + "]";
  }
}
