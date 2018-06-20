package com.obsidiandynamics.blackstrom.ledger;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.jackdaw.*;

public final class MockKafkaLedger {
  private MockKafkaLedger() {}
  
  public static KafkaLedger create() {
    return create(__ -> {});
  }
  
  public static KafkaLedger create(Consumer<KafkaLedgerConfig> configConsumer) {
    final KafkaLedgerConfig config = new KafkaLedgerConfig()
        .withKafka(new MockKafka<>())
        .withTopic("mock")
        .withCodec(new NullMessageCodec());
    configConsumer.accept(config);
    return new KafkaLedger(config);
  }
}
