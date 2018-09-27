package com.obsidiandynamics.blackstrom.ledger;

public interface KafkaDefaults {
  static int CLUSTER_AWAIT = 120_000;
  static int CONSUMER_GROUP_OPERATION = 30_000;
  static int TOPIC_OPERATION = 30_000;
  static final String TOPIC_DATE_FORMAT = "yyyy-MM-dd-HHmmss";
}
