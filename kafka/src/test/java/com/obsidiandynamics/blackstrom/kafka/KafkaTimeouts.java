package com.obsidiandynamics.blackstrom.kafka;

public interface KafkaTimeouts {
  static long CLUSTER_AWAIT = 120_000;
  static long TOPIC_CREATE = 10_000;
}
