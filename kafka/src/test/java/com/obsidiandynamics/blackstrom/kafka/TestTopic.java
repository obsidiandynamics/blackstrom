package com.obsidiandynamics.blackstrom.kafka;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.concat.*;

public final class TestTopic {
  private TestTopic() {}
  
  public static String of(Class<?> testClass, String codecName, int encodingVersion, Object... extras) {
    return new Concat()
    .append(testClass.getSimpleName())
    .append(".")
    .append(codecName)
    .append("-")
    .append(encodingVersion)
    .append(".v")
    .append(MessageCodec.SCHEMA_VERSION)
    .when(extras.length != 0).append(".").appendArray(".", extras)
    .toString();
  }
}
