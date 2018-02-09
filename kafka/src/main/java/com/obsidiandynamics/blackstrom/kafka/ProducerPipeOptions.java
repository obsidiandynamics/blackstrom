package com.obsidiandynamics.blackstrom.kafka;

public final class ProducerPipeOptions {
  private boolean async;
  
  public ProducerPipeOptions withAsync(boolean async) {
    this.async = async;
    return this;
  }
  
  boolean isAsync() {
    return async;
  }
  
  @Override
  public String toString() {
    return ProducerPipeOptions.class.getSimpleName() + " [async=" + async + "]";
  }
}
