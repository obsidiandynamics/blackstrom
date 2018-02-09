package com.obsidiandynamics.blackstrom.kafka;

import static java.lang.String.*;

import com.obsidiandynamics.blackstrom.codec.*;

public final class TestTopic {
  private TestTopic() {}
  
  public static String of(Class<?> testClass, String codecName, int encodingVersion) {
    return format("%s.%s-%d.v%d", 
                  testClass.getSimpleName(), codecName, encodingVersion, MessageCodec.SCHEMA_VERSION);
  }
}
