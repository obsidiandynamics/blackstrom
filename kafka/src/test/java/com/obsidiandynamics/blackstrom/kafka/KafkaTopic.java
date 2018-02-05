package com.obsidiandynamics.blackstrom.kafka;

import com.obsidiandynamics.blackstrom.codec.*;

public final class KafkaTopic {
  private KafkaTopic() {}
  
  public static String forTest(Class<?> testClass, String codecName) {
    return testClass.getSimpleName() + "." + codecName + ".v" + MessageCodec.SCHEMA_VERSION;
  }
}
