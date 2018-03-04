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
import org.slf4j.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

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
                                          int pipelineSizeBatches, Logger log) {
    return createLedger(kafka, new KafkaLedgerConfig(), asyncProducer, asyncConsumer, pipelineSizeBatches, log);
  }
  
  private static KafkaLedger createLedger(Kafka<String, Message> kafka, 
                                          KafkaLedgerConfig baseConfig,
                                          boolean asyncProducer, boolean asyncConsumer, 
                                          int pipelineSizeBatches, Logger log) {
    return new KafkaLedger(baseConfig
                           .withKafka(kafka)
                           .withTopic("test")
                           .withCodec(new NullMessageCodec())
                           .withLog(log)
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
                          false, true, 1, LoggerFactory.getLogger(KafkaLedger.class));
    final CyclicBarrier barrierA = new CyclicBarrier(2);
    final CyclicBarrier barrierB = new CyclicBarrier(2);
    final AtomicInteger received = new AtomicInteger();
    ledger.attach(new NullGroupMessageHandler() {
      @Override public void onMessage(MessageContext context, Message message) {
        if (received.get() == 0) {
          TestSupport.await(barrierA);
          TestSupport.await(barrierB);
        }
        received.incrementAndGet();
      }
    });
    
    ledger.append(new Proposal("B100", new String[0], null, 0));
    TestSupport.await(barrierA);
    ledger.append(new Proposal("B200", new String[0], null, 0));
    TestSupport.sleep(50);
    ledger.append(new Proposal("B300", new String[0], null, 0));
    TestSupport.sleep(50);
    TestSupport.await(barrierB);
    wait.until(() -> assertEquals(3, received.get()));
  }
  
  @Test
  public void testSendCallbackExceptionLoggerPass() {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("testSendExceptionLoggerPass");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(ExceptionGenerator.never());
    ledger = createLedger(kafka, false, true, 10, log);
    ledger.append(new Proposal("B100", new String[0], null, 0));
    verify(log, never()).warn(isNotNull(), eq(exception));
  }
  
  @Test
  public void testSendCallbackExceptionLoggerFail() {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("testSendExceptionLoggerFail");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, log);
    ledger.append(new Proposal("B100", new String[0], null, 0), (id, x) -> {});
    
    wait.until(() -> {
      verify(log).warn(isNotNull(), eq(exception));
    });
  }
  
  @Test
  public void testSendCallbackRetriableException() {
    final Logger log = mock(Logger.class);
    final Exception exception = new CorruptRecordException("testSendRetriableException");
    final ExceptionGenerator<ProducerRecord<String, Message>, Exception> exGen = ExceptionGenerator.times(exception, 2);
    final ExceptionGenerator<ProducerRecord<String, Message>, Exception> mockExGen = Cast.from(mock(ExceptionGenerator.class));
    when(mockExGen.get(any())).thenAnswer(invocation -> exGen.get(invocation.getArgument(0)));
    
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(mockExGen);
    ledger = createLedger(kafka, false, true, 10, log);
    ledger.append(new Proposal("B100", new String[0], null, 0), (id, x) -> {});
    
    wait.until(() -> {
      verify(log, times(2)).warn(isNotNull(), eq(exception));
      verify(mockExGen, times(3)).get(any());
    });
  }
  
  @Test
  public void testSendRuntimeException() {
    final Logger log = mock(Logger.class);
    final IllegalStateException exception = new IllegalStateException("testSendRuntimeException");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendRuntimeExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, log);
    ledger.append(new Proposal("B100", new String[0], null, 0), (id, x) -> {});
    wait.until(() -> {
      verify(log).error(isNotNull(), (Throwable) isNotNull());
    });
  }
  
  @Test
  public void testCommitCallbackExceptionLoggerFail() {
    final Logger log = mock(Logger.class);
    final Exception exception = new Exception("testCommitExceptionLoggerFail");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withCommitExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, log);
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
      verify(log, atLeastOnce()).warn(isNotNull(), eq(exception));
    });
  }
  
  @Test
  public void testConfirmDirectNoHandlerId() {
    final Kafka<String, Message> kafka = new MockKafka<>();
    final Logger log = mock(Logger.class);
    ledger = createLedger(kafka, false, true, 10, log);
    ledger.confirm(null, null);
  }
  
  @Test
  public void testAppendAfterDispose() {
    ledger = MockKafkaLedger.create();
    ledger.dispose();
    final AppendCallback callback = mock(AppendCallback.class);
    ledger.append(new Proposal("B100", new String[0], null, 0), callback);
    TestSupport.sleep(10);
    verifyNoMoreInteractions(callback);
  }
}
