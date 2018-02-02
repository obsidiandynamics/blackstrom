package com.obsidiandynamics.blackstrom.factor;

public final class DelayedDelivery extends FailureMode {
  private final int delayMillis;
  
  public DelayedDelivery(double probability, int delayMillis) {
    super(probability);
    this.delayMillis = delayMillis;
  }
  
  public int getDelayMillis() {
    return delayMillis;
  }

  @Override
  public FailureType getFailureType() {
    return FailureType.DELAYED_DELIVERY;
  }

  @Override
  public String toString() {
    return DelayedDelivery.class.getSimpleName() + " [probability=" + getProbability() + 
        ", delayMillis=" + delayMillis + "]";
  }
}
