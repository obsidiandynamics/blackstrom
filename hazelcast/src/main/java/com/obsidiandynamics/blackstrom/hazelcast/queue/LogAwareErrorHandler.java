package com.obsidiandynamics.blackstrom.hazelcast.queue;

import java.util.function.*;

import org.slf4j.*;

final class LogAwareErrorHandler implements ErrorHandler {
  private final Supplier<Logger> logSupplier;
  
  LogAwareErrorHandler(Supplier<Logger> logSupplier) {
    this.logSupplier = logSupplier;
  }

  @Override
  public void onError(String summary, Throwable error) {
    logSupplier.get().warn(summary, error);
  }
}
