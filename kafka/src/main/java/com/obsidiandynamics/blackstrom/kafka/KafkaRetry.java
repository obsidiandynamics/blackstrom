package com.obsidiandynamics.blackstrom.kafka;

import org.apache.kafka.common.*;
import org.slf4j.*;

public final class KafkaRetry {
  private KafkaRetry() {}
  
  public static void run(int attempts, Logger log, Runnable r) {
    for (int attempt = 0;; attempt++) {
      try {
        r.run();
        break;
      } catch (KafkaException e) {
        if (attempt == attempts - 1) {
          log.error("Error", e);
          throw e;
        } else {
          log.warn("Error: {} (attempt #{} of {})", e, attempt + 1, attempts);
        }
      }
    }
  }
}
