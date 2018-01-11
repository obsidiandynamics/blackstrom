package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaGroupLedgerIT {
//  @Override
  protected Ledger createLedgerImpl() {
    final Kafka<String, Message> kafka = 
        new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers("localhost:9092"));
    return new KafkaLedger(kafka, KafkaGroupLedgerIT.class.getSimpleName());
  }
}
