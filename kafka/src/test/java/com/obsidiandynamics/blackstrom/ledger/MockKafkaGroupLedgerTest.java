package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class MockKafkaGroupLedgerTest extends AbstractGroupLedgerTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @Override
  protected Ledger createLedgerImpl() {
    final Kafka<String, Message> kafka = new MockKafka<>();
    return new KafkaLedger(kafka, "test");
  }
}
