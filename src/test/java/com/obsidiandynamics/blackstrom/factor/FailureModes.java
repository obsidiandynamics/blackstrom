package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.util.*;

public final class FailureModes {
  private final FailureType failureType;
  
  private final double probability;
  
  private final Object extent;
  
  public FailureModes(FailureType failureType, double probability) {
    this(failureType, probability, null);
  }
  
  public FailureModes(FailureType failureType, double probability, Object extent) {
    this.failureType = failureType;
    this.probability = probability;
    this.extent = extent;
  }

  public FailureType getFailureType() {
    return failureType;
  }
  
  public double getProbability() {
    return probability;
  }
  
  public <X> X getExtent() {
    return Cast.from(extent);
  }
  
  public boolean isTime() {
    return Math.random() < probability;
  }

  @Override
  public String toString() {
    return "FailureMode [failureType=" + failureType + ", probability=" + probability + ", extent=" + extent + "]";
  }
}
