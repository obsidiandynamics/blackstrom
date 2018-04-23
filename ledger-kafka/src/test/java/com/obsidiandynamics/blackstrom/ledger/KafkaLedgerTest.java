package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;
import com.obsidiandynamics.zerolog.MockLogTarget.*;

@RunWith(Parameterized.class)
public final class KafkaLedgerTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final Timesert wait = Wait.SHORT;
  
  private KafkaLedger ledger;
  
  @After
  public void after() {
    if (ledger != null) ledger.dispose();
  }
  
  private static KafkaLedger createLedger(Kafka<String, Message> kafka, 
                                          boolean asyncProducer, boolean asyncConsumer, 
                                          int pipelineSizeBatches, Zlg zlg) {
    return createLedger(kafka, new KafkaLedgerConfig(), asyncProducer, asyncConsumer, pipelineSizeBatches, zlg);
  }
  
  private static KafkaLedger createLedger(Kafka<String, Message> kafka, 
                                          KafkaLedgerConfig baseConfig,
                                          boolean asyncProducer, boolean asyncConsumer, 
                                          int pipelineSizeBatches, Zlg zlg) {
    return new KafkaLedger(baseConfig
                           .withKafka(kafka)
                           .withTopic("test")
                           .withCodec(new NullMessageCodec())
                           .withZlg(zlg)
                           .withProducerPipeConfig(new ProducerPipeConfig()
                                                   .withAsync(asyncProducer))
                           .withConsumerPipeConfig(new ConsumerPipeConfig()
                                                   .withAsync(asyncConsumer)
                                                   .withBacklogBatches(pipelineSizeBatches)));
  }
  
  @Test
  public void testPipelineBackoff() {
    final Kafka<String, Message> kafka = new MockKafka<>();
    ledger = createLedger(kafka, new KafkaLedgerConfig().withPrintConfig(true), 
                          false, true, 1, new MockLogTarget().logger());
    final CyclicBarrier barrierA = new CyclicBarrier(2);
    final CyclicBarrier barrierB = new CyclicBarrier(2);
    final AtomicInteger received = new AtomicInteger();
    ledger.attach(new NullGroupMessageHandler() {
      @Override public void onMessage(MessageContext context, Message message) {
        if (received.get() == 0) {
          Threads.await(barrierA);
          Threads.await(barrierB);
        }
        received.incrementAndGet();
      }
    });
    
    ledger.append(new Proposal("B100", new String[0], null, 0));
    Threads.await(barrierA);
    ledger.append(new Proposal("B200", new String[0], null, 0));
    Threads.sleep(50);
    ledger.append(new Proposal("B300", new String[0], null, 0));
    Threads.sleep(50);
    Threads.await(barrierB);
    wait.until(() -> assertEquals(3, received.get()));
  }
  
  @Test
  public void testSendCallbackExceptionLoggerPass() {
    final MockLogTarget logTarget = new MockLogTarget();
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(ExceptionGenerator.never());
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0));
    assertEquals(0, logTarget.entries().forLevel(LogLevel.WARN).list().size());
  }
  
  @Test
  public void testSendCallbackExceptionLoggerFail() {
    final MockLogTarget logTarget = new MockLogTarget();
    final Exception exception = new Exception("testSendExceptionLoggerFail");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());
    
    wait.until(() -> {
      final List<Entry> entries = logTarget.entries().forLevel(LogLevel.WARN).filter(e -> e.getThrowable() == exception).list();
      assertEquals(1, entries.size());
    });
  }
  
  @Test
  public void testSendCallbackRetriableException() {
    final MockLogTarget logTarget = new MockLogTarget();
    final Exception exception = new CorruptRecordException("testSendRetriableException");
    final ExceptionGenerator<ProducerRecord<String, Message>, Exception> exGen = ExceptionGenerator.times(exception, 2);
    final ExceptionGenerator<ProducerRecord<String, Message>, Exception> mockExGen = Classes.cast(mock(ExceptionGenerator.class));
    when(mockExGen.inspect(any())).thenAnswer(invocation -> exGen.inspect(invocation.getArgument(0)));
    
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(mockExGen);
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());
    
    wait.until(() -> {
      final List<Entry> entries = logTarget.entries().forLevel(LogLevel.WARN).filter(e -> e.getThrowable() == exception).list();
      assertEquals(2, entries.size());
      verify(mockExGen, times(3)).inspect(any());
    });
  }
  
  @Test
  public void testSendRuntimeException() {
    final MockLogTarget logTarget = new MockLogTarget();
    final IllegalStateException exception = new IllegalStateException("testSendRuntimeException");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendRuntimeExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());
    wait.until(() -> {
      final List<Entry> entries = logTarget.entries().forLevel(LogLevel.WARN).filter(e -> e.getThrowable() == exception).list();
      assertEquals(1, entries.size());
    });
  }
  
  @Test
  public void testCommitCallbackExceptionLoggerFail() {
    final MockLogTarget logTarget = new MockLogTarget();
    final Exception exception = new Exception("testCommitExceptionLoggerFail");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withCommitExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    final String groupId = "test";
    
    ledger.attach(new MessageHandler() {
      @Override
      public String getGroupId() {
        return groupId;
      }

      @Override
      public void onMessage(MessageContext context, Message message) {
        try {
          context.beginAndConfirm(message);
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    });
    
    wait.until(() -> {
      ledger.append(new Proposal("B100", new String[0], null, 0));
      final List<Entry> entries = logTarget.entries().forLevel(LogLevel.WARN).list();
      assertEquals(1, entries.size());
      assertEquals(exception, entries.get(0).getThrowable());
    });
  }
  
  @Test
  public void testAppendAfterDispose() {
    ledger = MockKafkaLedger.create();
    ledger.dispose();
    final AppendCallback callback = mock(AppendCallback.class);
    ledger.append(new Proposal("B100", new String[0], null, 0), callback);
    Threads.sleep(10);
    verifyNoMoreInteractions(callback);
  }
}
