package com.obsidiandynamics.blackstrom.factor;

public abstract class FailureMode {
  public enum FailureType {
    DELAYED_DELIVERY,
    DUPLICATE_DELIVERY,
    DELAYED_DUPLICATE_DELIVERY
  }
  
  private final double probability;
  
  protected FailureMode(double probability) {
    this.probability = probability;
  }

  public abstract FailureType getFailureType();
  
  public final double getProbability() {
    return probability;
  }
  
  public final boolean isTime() {
    return Math.random() < probability;
  }
}
