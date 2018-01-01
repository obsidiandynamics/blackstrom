package com.obsidiandynamics.blackstrom.factor;

public final class DuplicateDelivery extends FailureMode {
  public DuplicateDelivery(double probability) {
    super(probability);
  }

  @Override
  public FailureType getFailureType() {
    return FailureType.DUPLICATE_DELIVERY;
  }

  @Override
  public String toString() {
    return DuplicateDelivery.class.getSimpleName() + " [probability=" + getProbability() + "]";
  }
}
