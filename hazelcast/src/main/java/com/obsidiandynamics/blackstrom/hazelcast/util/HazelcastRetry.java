package com.obsidiandynamics.blackstrom.hazelcast.util;

import java.util.function.*;

import org.slf4j.*;

import com.hazelcast.core.*;

public final class HazelcastRetry {
  private static final Logger defaultLog = LoggerFactory.getLogger(HazelcastRetry.class);
  
  private int attempts = 10;
  private int backoffMillis = 100;
  private Logger log = defaultLog;
  
  public HazelcastRetry withAttempts(int attempts) {
    this.attempts = attempts;
    return this;
  }
  
  public HazelcastRetry withBackoffMillis(int backoffMillis) {
    this.backoffMillis = backoffMillis;
    return this;
  }
  
  public HazelcastRetry withLog(Logger log) {
    this.log = log;
    return this;
  }

  @Override
  public String toString() {
    return HazelcastRetry.class.getSimpleName() + " [attempts=" + attempts + ", backoffMillis=" + backoffMillis + ", log=" + log + "]";
  }
  
  public void run(Runnable operation) {
    run(toVoidSupplier(operation));
  }
  
  private static Supplier<Void> toVoidSupplier(Runnable r) {
    return () -> {
      r.run();
      return null;
    };
  }
  
  public <T> T run(Supplier<? extends T> operation) {
    for (int attempt = 0;; attempt++) {
      try {
        return operation.get();
      } catch (HazelcastException e) {
        final String message = String.format("Error: (attempt #%,d of %,d)", e, attempt + 1, attempts);
        if (attempt == attempts - 1) {
          log.error(message, e);
          throw e;
        } else {
          log.warn(message, e);
          sleepWithInterrupt(backoffMillis);
        }
      }
    }
  }
  
  public static void sleepWithInterrupt(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
