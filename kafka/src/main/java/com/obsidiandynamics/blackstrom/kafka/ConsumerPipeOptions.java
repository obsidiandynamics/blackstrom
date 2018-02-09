package com.obsidiandynamics.blackstrom.kafka;

public final class ConsumerPipeOptions {
  private boolean async = true;
  
  private int backlogBatches = 10;
  
  public ConsumerPipeOptions withAsync(boolean async) {
    this.async = async;
    return this;
  }
  
  boolean isAsync() {
    return async;
  }
  
  public ConsumerPipeOptions withBacklogBatches(int backlogBatches) {
    this.backlogBatches = backlogBatches;
    return this;
  }
  
  int getBacklogBatches() {
    return backlogBatches;
  }
  
  @Override
  public String toString() {
    return ConsumerPipeOptions.class.getSimpleName() + " [async=" + async + ", backlogBatches=" + backlogBatches + "]";
  }
}
