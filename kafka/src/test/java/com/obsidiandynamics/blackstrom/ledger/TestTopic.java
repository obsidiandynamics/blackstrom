package com.obsidiandynamics.blackstrom.ledger;

import org.apache.kafka.clients.admin.*;

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
  
  public static NewTopic newOf(String topic) {
    return new NewTopic(topic, (short) 1, (short) 1);
  }
}
