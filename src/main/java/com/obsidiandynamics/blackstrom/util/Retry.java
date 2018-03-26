package com.obsidiandynamics.blackstrom.util;

import java.util.function.*;

import org.slf4j.*;

public final class Retry {
  private static final Logger defaultLog = LoggerFactory.getLogger(Retry.class);
  
  private Class<? extends RuntimeException> exceptionClass = RuntimeException.class;
  private int attempts = 10;
  private int backoffMillis = 100;
  private Logger log = defaultLog;
  
  public Retry withExceptionClass(Class<? extends RuntimeException> exceptionClass) {
    this.exceptionClass = exceptionClass;
    return this;
  }
  
  public Retry withAttempts(int attempts) {
    this.attempts = attempts;
    return this;
  }
  
  public Retry withBackoffMillis(int backoffMillis) {
    this.backoffMillis = backoffMillis;
    return this;
  }
  
  public Retry withLog(Logger log) {
    this.log = log;
    return this;
  }

  @Override
  public String toString() {
    return Retry.class.getSimpleName() + " [attempts=" + attempts + ", backoffMillis=" + backoffMillis 
        + ", log=" + log + ", exceptionClass=" + exceptionClass.getSimpleName() + "]";
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
      } catch (RuntimeException e) {
        if (exceptionClass.isInstance(e)) {
          final String message = String.format("Fault: (attempt #%,d of %,d)", attempt + 1, attempts);
          if (attempt == attempts - 1) {
            log.error(message, e);
            throw e;
          } else {
            if (sleepWithInterrupt(backoffMillis)) {
              log.warn(message, e);
            } else {
              log.error(message, e);
              throw e;
            }
          }
        } else {
          throw e;
        }
      }
    }
  }
  
  public static boolean sleepWithInterrupt(long millis) {
    if (millis > 0) {
      try {
        Thread.sleep(millis);
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    } else {
      return ! Thread.currentThread().isInterrupted();
    }
  }
}
