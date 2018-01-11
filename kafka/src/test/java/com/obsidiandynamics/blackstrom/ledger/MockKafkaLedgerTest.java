package com.obsidiandynamics.blackstrom.ledger;

import java.util.logging.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class MockKafkaLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedgerImpl() {
    final Kafka<String, Message> kafka = new MockKafka<>();
    return new KafkaLedger(kafka, "test");
  }
  
//  @Test
//  public void testExceptionLoggerNoException() {
//    final Logger log = Mockito.mock(Logger.class);
//    
//    KafkaLedger.logException(log, cause, messageFormat, messageArgs);
//  }
}
