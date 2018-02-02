package com.obsidiandynamics.blackstrom.factor;

public final class DelayedDuplicateDelivery extends FailureMode {
  private final int delayMillis;
  
  public DelayedDuplicateDelivery(double probability, int delayMillis) {
    super(probability);
    this.delayMillis = delayMillis;
  }
  
  public int getDelayMillis() {
    return delayMillis;
  }

  @Override
  public FailureType getFailureType() {
    return FailureType.DELAYED_DUPLICATE_DELIVERY;
  }

  @Override
  public String toString() {
    return DelayedDuplicateDelivery.class.getSimpleName() + " [probability=" + getProbability() + 
        ", delayMillis=" + delayMillis + "]";
  }
}
