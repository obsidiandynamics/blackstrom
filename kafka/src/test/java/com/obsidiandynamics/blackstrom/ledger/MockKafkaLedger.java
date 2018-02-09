package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;

public final class MockKafkaLedger {
  private MockKafkaLedger() {}
  
  public static KafkaLedger create() {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withKafka(new MockKafka<>())
                           .withTopic("mock")
                           .withCodec(new NullMessageCodec()));
  }
}
