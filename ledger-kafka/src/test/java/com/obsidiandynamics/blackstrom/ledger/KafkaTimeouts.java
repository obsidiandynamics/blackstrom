package com.obsidiandynamics.blackstrom.ledger;

public interface KafkaTimeouts {
  static int CLUSTER_AWAIT = 120_000;
  static int TOPIC_OPERATION = 10_000;
}
