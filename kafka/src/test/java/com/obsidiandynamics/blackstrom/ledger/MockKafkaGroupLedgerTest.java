package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class MockKafkaGroupLedgerTest extends AbstractGroupLedgerTest {
  @Override
  protected Ledger createLedgerImpl() {
    final Kafka<String, Message> kafka = new MockKafka<>();
    return new KafkaLedger(kafka, "test");
  }
}
