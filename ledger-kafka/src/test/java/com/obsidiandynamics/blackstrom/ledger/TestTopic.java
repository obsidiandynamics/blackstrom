package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.retry.Retry.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.concat.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.retry.*;
import com.obsidiandynamics.zerolog.*;

public final class TestTopic {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();

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
    return newOf(topic, 1);
  }

  public static NewTopic newOf(String topic, int numPartitions) {
    return newOf(topic, numPartitions, (short) 1);
  }

  public static NewTopic newOf(String topic, int numPartitions, short replicationFactor) {
    return new NewTopic(topic, numPartitions, replicationFactor);
  }

  public static Set<String> createTopics(KafkaAdmin admin, NewTopic... newTopics) throws Exception {
    return new Retry()
        .withAttempts(10)
        .withBackoff(10_000)
        .withFaultHandler(zlg::w)
        .withErrorHandler(zlg::e)
        .withExceptionMatcher(isA(TimeoutException.class).or(isA(ExecutionException.class).and(hasCauseThat(isA(UnknownServerException.class)))))
        .run(() -> admin.createTopics(List.of(newTopics), KafkaDefaults.TOPIC_OPERATION));
  }
}
