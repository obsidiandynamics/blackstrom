package com.obsidiandynamics.blackstrom.kafka;

public final class ProducerPipeConfig {
  private boolean async;
  
  public ProducerPipeConfig withAsync(boolean async) {
    this.async = async;
    return this;
  }
  
  boolean isAsync() {
    return async;
  }
  
  @Override
  public String toString() {
    return ProducerPipeConfig.class.getSimpleName() + " [async=" + async + "]";
  }
}
