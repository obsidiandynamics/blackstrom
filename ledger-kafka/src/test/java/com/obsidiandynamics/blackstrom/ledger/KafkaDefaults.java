package com.obsidiandynamics.blackstrom.ledger;

public interface KafkaDefaults {
  int CLUSTER_AWAIT = 120_000;
  int CONSUMER_GROUP_OPERATION = 30_000;
  int TOPIC_OPERATION = 30_000;
  String TOPIC_DATE_FORMAT = "yyyy-MM-dd-HHmmss";
}
