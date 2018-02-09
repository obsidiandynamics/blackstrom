package com.obsidiandynamics.blackstrom.kafka;

public final class ConsumerPipeConfig {
  private boolean async = true;
  
  private int backlogBatches = 10;
  
  public ConsumerPipeConfig withAsync(boolean async) {
    this.async = async;
    return this;
  }
  
  boolean isAsync() {
    return async;
  }
  
  public ConsumerPipeConfig withBacklogBatches(int backlogBatches) {
    this.backlogBatches = backlogBatches;
    return this;
  }
  
  int getBacklogBatches() {
    return backlogBatches;
  }
  
  @Override
  public String toString() {
    return ConsumerPipeConfig.class.getSimpleName() + " [async=" + async + ", backlogBatches=" + backlogBatches + "]";
  }
}
