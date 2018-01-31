package com.obsidiandynamics.blackstrom.ledger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.slf4j.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class MockKafkaLedgerTest extends AbstractLedgerTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.SHORT;
  }
  
  @Override
  protected Ledger createLedger() {
    return MockKafkaLedger.create();
  }
  
  @Test
  public void testSendExceptionLoggerPass() {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("testSendExceptionLoggerPass");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withAppendExceptionGenerator(ExceptionGenerator.never());
    final KafkaLedger ledger = new KafkaLedger(kafka, "test", false).withLogger(log);
    try {
      ledger.append(new Proposal("B100", new String[0], null, 0));
      verify(log, never()).warn(isNotNull(), eq(exception));
    } finally {
      ledger.dispose();
    }
  }
  
  @Test
  public void testSendExceptionLoggerFail() {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("testSendExceptionLoggerFail");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withAppendExceptionGenerator(ExceptionGenerator.once(exception));
    final KafkaLedger ledger = new KafkaLedger(kafka, "test", false).withLogger(log);
    try {
      ledger.append(new Proposal("B100", new String[0], null, 0), (id, x) -> {});
      verify(log).warn(isNotNull(), eq(exception));
    } finally {
      ledger.dispose();
    }
  }
  
  @Test
  public void testCommitExceptionLoggerFail() {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("testCommitExceptionLoggerFail");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withConfirmExceptionGenerator(ExceptionGenerator.once(exception));
    final KafkaLedger ledger = new KafkaLedger(kafka, "test", false).withLogger(log);
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
            context.confirm(new DefaultMessageId(0, 0));
          } catch (Throwable e) {
            e.printStackTrace();
          } finally {
            TestSupport.await(barrier);
          }
        }
      });
      ledger.append(new Proposal("B100", new String[0], null, 0));
  
      TestSupport.await(barrier);
      wait.until(() -> {
        verify(log).warn(isNotNull(), eq(exception));
      });
    } finally {
      ledger.dispose();
    }
  }
  
  @Test
  public void testAppendAfterDispose() {
    final KafkaLedger ledger = MockKafkaLedger.create();
    ledger.dispose();
    final AppendCallback callback = mock(AppendCallback.class);
    ledger.append(new Proposal("B100", new String[0], null, 0), callback);
    TestSupport.sleep(10);
    verifyNoMoreInteractions(callback);
  }
}
