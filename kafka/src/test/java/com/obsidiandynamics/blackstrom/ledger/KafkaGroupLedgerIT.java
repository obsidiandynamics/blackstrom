package com.obsidiandynamics.blackstrom.ledger;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class KafkaGroupLedgerIT extends AbstractGroupLedgerTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaDocker.start();
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.MEDIUM;
  }
  
  @Override
  protected Ledger createLedgerImpl() {
    final Kafka<String, Message> kafka = 
        new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers("localhost:9092"));
    return new KafkaLedger(kafka, KafkaGroupLedgerIT.class.getSimpleName() + ".v2");
  }
}
