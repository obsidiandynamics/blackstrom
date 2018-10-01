package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.KafkaLedger.*;
import com.obsidiandynamics.blackstrom.ledger.KafkaLedger.ConsumerState.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;

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
    final KafkaLedgerConfig ledgerConfig = new KafkaLedgerConfig().withMaxConsumerPipeYields(1);
    return createLedger(kafka, ledgerConfig, asyncProducer, asyncConsumer, pipelineSizeBatches, zlg);
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
                           .withDrainConfirmations(true)
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
      @Override 
      public void onMessage(MessageContext context, Message message) {
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
    final MockLogTarget target = new MockLogTarget();
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(ExceptionGenerator.never());
    ledger = createLedger(kafka, false, true, 10, target.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0));
    assertEquals(0, target.entries().forLevel(LogLevel.WARN).list().size());
  }
  
  @Test
  public void testSendCallbackExceptionLoggerFail() {
    final MockLogTarget target = new MockLogTarget();
    final Exception exception = new Exception("simulated");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, target.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());
    
    wait.until(() -> {
      target.entries().forLevel(LogLevel.WARN).withThrowable(exception).assertCount(1);
    });
  }
  
  @Test
  public void testSendCallbackRetriableException() {
    final MockLogTarget target = new MockLogTarget();
    final Exception exception = new CorruptRecordException("simulated");
    final ExceptionGenerator<ProducerRecord<String, Message>, Exception> exGen = ExceptionGenerator.times(exception, 2);
    final ExceptionGenerator<ProducerRecord<String, Message>, Exception> mockExGen = Classes.cast(mock(ExceptionGenerator.class));
    when(mockExGen.inspect(any())).thenAnswer(invocation -> exGen.inspect(invocation.getArgument(0)));
    
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(mockExGen);
    ledger = createLedger(kafka, false, true, 10, target.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());
    
    wait.until(() -> {
      target.entries().forLevel(LogLevel.WARN).withThrowable(exception).assertCount(2);
      verify(mockExGen, times(3)).inspect(any());
    });
  }
  
  @Test
  public void testSendRuntimeException() {
    final MockLogTarget target = new MockLogTarget();
    final IllegalStateException exception = new IllegalStateException("simulated");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withSendRuntimeExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, target.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());
    wait.until(() -> {
      target.entries().forLevel(LogLevel.WARN).withThrowable(exception).assertCount(1);
    });
  }
  
  @Test
  public void testCommitCallbackExceptionLoggerFail() {
    final MockLogTarget target = new MockLogTarget();
    final Exception exception = new Exception("simulated");
    final Kafka<String, Message> kafka = new MockKafka<String, Message>()
        .withCommitExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, target.logger());
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
      target.entries().forLevel(LogLevel.WARN).withThrowable(exception).assertCount(1);
    });
  }
  
  @Test
  public void testAttachAfterDispose() {
    ledger = MockKafkaLedger.create();
    assertFalse(ledger.isDisposing());
    ledger.dispose();
    assertTrue(ledger.isDisposing());
    
    final MessageHandler handler = mock(MessageHandler.class);
    ledger.attach(handler);

    ledger.append(new Proposal("B100", new String[0], null, 0));
    Threads.sleep(10);
    verifyNoMoreInteractions(handler);
  }
  
  @Test
  public void testAppendAfterDispose() {
    ledger = MockKafkaLedger.create();
    assertFalse(ledger.isDisposing());
    ledger.dispose();
    assertTrue(ledger.isDisposing());
    final AppendCallback callback = mock(AppendCallback.class);
    ledger.append(new Proposal("B100", new String[0], null, 0), callback);
    Threads.sleep(10);
    verifyNoMoreInteractions(callback);
  }
  
  @Test
  public void testHandleRecordWithNoConsumerState() {
    final MessageHandler handler = mock(MessageHandler.class);
    final Message message = new Command("xid", null, 0);
    final ConsumerRecord<String, Message> record = new ConsumerRecord<>("topic", 0, 0L, "key", message);
    final MockLogTarget logTarget = new MockLogTarget();
    
    KafkaLedger.handleRecord(handler, null, null, record, logTarget.logger());
    verify(handler).onMessage(isNull(), eq(message));
    logTarget.entries().assertCount(0);
  }
  
  @Test
  public void testHandleRecordWithAssignedTopic() {
    final MessageHandler handler = mock(MessageHandler.class);
    final ConsumerState consumerState = new ConsumerState();
    consumerState.assignedPartitions = new HashSet<>(Arrays.asList(0));
    final Message message = new Command("xid", null, 0);
    final ConsumerRecord<String, Message> record = new ConsumerRecord<>("topic", 0, 0L, "key", message);
    final MockLogTarget logTarget = new MockLogTarget();
    
    KafkaLedger.handleRecord(handler, consumerState, null, record, logTarget.logger());
    verify(handler).onMessage(isNull(), eq(message));
    logTarget.entries().assertCount(0);
  }
  
  @Test
  public void testHandleRecordWithMonotonicConstraintViolation() {
    final MessageHandler handler = mock(MessageHandler.class);
    final ConsumerState consumerState = new ConsumerState();
    consumerState.assignedPartitions = new HashSet<>(Arrays.asList(0));
    consumerState.offsetsProcessed.computeIfAbsent(0, __ -> new MutableOffset()).offset = 0;
    final Message message = new Command("xid", null, 0);
    final ConsumerRecord<String, Message> record = new ConsumerRecord<>("topic", 0, 0L, "key", message);
    final MockLogTarget logTarget = new MockLogTarget();
    
    KafkaLedger.handleRecord(handler, consumerState, null, record, logTarget.logger());
    verify(handler, never()).onMessage(isNull(), eq(message));
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG)
    .containing("Skipping message at offset 0, partition: 0: monotonicity constraint violated (previous offset 0)").assertCount(1);
  }
  
  @Test
  public void testHandleRecordWithUnassignedTopic() {
    final MessageHandler handler = mock(MessageHandler.class);
    final ConsumerState consumerState = new ConsumerState();
    final Message message = new Command("xid", null, 0);
    final ConsumerRecord<String, Message> record = new ConsumerRecord<>("topic", 0, 0L, "key", message);
    final MockLogTarget logTarget = new MockLogTarget();
    
    KafkaLedger.handleRecord(handler, consumerState, null, record, logTarget.logger());
    verify(handler, never()).onMessage(isNull(), eq(message));
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG)
    .containing("Skipping message at offset 0: partition 0 has been reassigned").assertCount(1);
  }
  
  @Test
  public void testConfirmItermediate() {
    final DefaultMessageId messageId = new DefaultMessageId(0, 100L);
    final ConsumerState consumerState = new ConsumerState();
    consumerState.offsetsPending.put(0, 200L);
    final MockLogTarget logTarget = new MockLogTarget();
    
    KafkaLedger.confirm(messageId, consumerState, "topic", logTarget.logger());
    assertTrue(consumerState.offsetsConfirmed.containsKey(new TopicPartition("topic", 0)));
    assertTrue(consumerState.offsetsConfirmed.containsValue(new OffsetAndMetadata(100L)));
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.TRACE).containing("intermediate").assertCount(1);
  }
  
  @Test
  public void testConfirmLast() {
    final DefaultMessageId messageId = new DefaultMessageId(0, 100L);
    final ConsumerState consumerState = new ConsumerState();
    consumerState.offsetsPending.put(0, 100L);
    final MockLogTarget logTarget = new MockLogTarget();
    
    KafkaLedger.confirm(messageId, consumerState, "topic", logTarget.logger());
    assertTrue(consumerState.offsetsConfirmed.containsKey(new TopicPartition("topic", 0)));
    assertTrue(consumerState.offsetsConfirmed.containsValue(new OffsetAndMetadata(100L)));
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.TRACE).containing("last").assertCount(1);
  }
  
  @Test
  public void testCommitOffsetsWithEnqueued() {
    final Consumer<String, Message> consumer = Classes.cast(mock(Consumer.class));
    final ConsumerState consumerState = new ConsumerState();
    final MockLogTarget logTarget = new MockLogTarget();
    final TopicPartition topicPartition = new TopicPartition("topic", 0);
    final OffsetAndMetadata offsetsAndMetadata = new OffsetAndMetadata(100L);
    consumerState.offsetsConfirmed.put(topicPartition, offsetsAndMetadata);
    final Map<TopicPartition, OffsetAndMetadata> offsetsConfirmed = consumerState.offsetsConfirmed;
    
    KafkaLedger.commitOffsets(consumer, consumerState, logTarget.logger());
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.TRACE).containing("Committing offsets").assertCount(1);
    verify(consumer).commitAsync(eq(offsetsConfirmed), isNotNull());
  }
  
  @Test
  public void testCommitOffsetsWithNoneEnqueued() {
    final Consumer<String, Message> consumer = Classes.cast(mock(Consumer.class));
    final ConsumerState consumerState = new ConsumerState();
    final MockLogTarget logTarget = new MockLogTarget();
    
    KafkaLedger.commitOffsets(consumer, consumerState, logTarget.logger());
    logTarget.entries().assertCount(0);
    verify(consumer, never()).commitAsync(any(), isNotNull());
  }
  
  @Test
  public void testSleepFor() {
    KafkaLedger.sleepFor(0).run();
  }
  
  @Test
  public void testDrainOffsetsWithNoPendingOffsets() {
    final Consumer<String, Message> consumer = Classes.cast(mock(Consumer.class));
    final ConsumerState consumerState = new ConsumerState();
    final MockLogTarget logTarget = new MockLogTarget();
    final Runnable intervalSleep = mock(Runnable.class);
    
    final AtomicBoolean disposing = new AtomicBoolean();
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 10_000, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("All offsets confirmed").assertCount(1);
    verify(consumer, never()).commitSync(Classes.<Map<TopicPartition, OffsetAndMetadata>>cast(any(Map.class)));
  }
  
  @Test
  public void testDrainOffsetsWithPendingOffsets() {
    final Consumer<String, Message> consumer = Classes.cast(mock(Consumer.class));
    final ConsumerState consumerState = new ConsumerState();
    final MockLogTarget logTarget = new MockLogTarget();
    final Runnable intervalSleep = mock(Runnable.class);
    consumerState.offsetsAccepted.put(0, 100L);
    consumerState.offsetsPending.put(0, 100L);
    doAnswer(__ -> {
      synchronized (consumerState.lock) {
        consumerState.offsetsPending.clear();
      }
      return null;
    }).when(intervalSleep).run();
    
    final AtomicBoolean disposing = new AtomicBoolean();
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 10_000, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(2);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("All offsets confirmed").assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("Offsets pending").assertCount(1);
    final Map<TopicPartition, OffsetAndMetadata> expectedOffsets = MapBuilder
        .init(new TopicPartition("topic", 0), new OffsetAndMetadata(100L))
        .build();
    verify(consumer).commitSync(eq(expectedOffsets));
  }
  
  @Test
  public void testDrainOffsetsWithPendingOffsetsTimeout() {
    final Consumer<String, Message> consumer = Classes.cast(mock(Consumer.class));
    final ConsumerState consumerState = new ConsumerState();
    final MockLogTarget logTarget = new MockLogTarget();
    final Runnable intervalSleep = mock(Runnable.class);
    consumerState.offsetsAccepted.put(0, 100L);
    consumerState.offsetsPending.put(0, 100L);
    
    final AtomicBoolean disposing = new AtomicBoolean();
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 0, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(2);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("Offsets pending").assertCount(1);
    logTarget.entries().forLevel(LogLevel.WARN).containing("Drain timed out (0 ms). Pending offsets");
    verify(consumer, never()).commitSync(Classes.<Map<TopicPartition, OffsetAndMetadata>>cast(any(Map.class)));
  }
  
  @Test
  public void testDrainOffsetsWhileDisposing() {
    final Consumer<String, Message> consumer = Classes.cast(mock(Consumer.class));
    final ConsumerState consumerState = new ConsumerState();
    final MockLogTarget logTarget = new MockLogTarget();
    final Runnable intervalSleep = mock(Runnable.class);
    
    final AtomicBoolean disposing = new AtomicBoolean(true);
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 10_000, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(0);
    verify(consumer, never()).commitSync(Classes.<Map<TopicPartition, OffsetAndMetadata>>cast(any(Map.class)));
  }
}
