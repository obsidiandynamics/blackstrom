package com.obsidiandynamics.blackstrom.ledger;

public final class DelayedFailure {
  private final int delayMillis;

  public DelayedFailure(int delayMillis) {
    this.delayMillis = delayMillis;
  }

  public int getDelayMillis() {
    return delayMillis;
  }

  @Override
  public String toString() {
    return "DelayedFailure [delayMillis=" + delayMillis + "]";
  }
}
