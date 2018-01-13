package com.obsidiandynamics.blackstrom.ledger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.*;

import org.junit.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public final class MockKafkaLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedgerImpl() {
    final Kafka<String, Message> kafka = new MockKafka<>();
    return new KafkaLedger(kafka, "test");
  }
  
  @Test
  public void testSendExceptionLoggerPass() throws Exception {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("Boom");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withAppendExceptionGenerator(ExceptionGenerator.never());
    final KafkaLedger ledger = new KafkaLedger(kafka, "test").withLogger(log);
    try {
      ledger.append(new Nomination(100, new String[0], null, 0));
      verify(log, never()).warn(isNotNull(), eq(exception));
    } finally {
      ledger.dispose();
    }
  }
  
  @Test
  public void testSendExceptionLoggerFail() throws Exception {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("Boom");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withAppendExceptionGenerator(ExceptionGenerator.once(exception));
    final KafkaLedger ledger = new KafkaLedger(kafka, "test").withLogger(log);
    try {
      ledger.append(new Nomination(100, new String[0], null, 0));
      verify(log).warn(isNotNull(), eq(exception));
    } finally {
      ledger.dispose();
    }
  }
  
  @Test
  public void testCommitExceptionLoggerFail() throws Exception {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("Boom");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withConfirmExceptionGenerator(ExceptionGenerator.once(exception));
    final KafkaLedger ledger = new KafkaLedger(kafka, "test").withLogger(log);
    try {
      final String groupId = "test";
      
      final CyclicBarrier barrier = new CyclicBarrier(2);
      ledger.attach(new MessageHandler() {
        @Override
        public String getGroupId() {
          return groupId;
        }
  
        @Override
        public void onMessage(MessageContext context, Message message) {
          try {
            context.confirm(new KafkaMessageId("test", 0, 0));
          } catch (Throwable e) {
            e.printStackTrace();
          } finally {
            TestSupport.await(barrier);
          }
        }
      });
      ledger.append(new Nomination(100, new String[0], null, 0));
  
      TestSupport.await(barrier);
      verify(log).warn(isNotNull(), eq(exception));
    } finally {
      ledger.dispose();
    }
  }
}