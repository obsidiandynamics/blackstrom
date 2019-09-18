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
import com.obsidiandynamics.jackdaw.AsyncReceiver.*;
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
                                          boolean asyncProducer, 
                                          boolean asyncConsumer, 
                                          int pipelineSizeBatches, 
                                          Zlg zlg) {
    final var ledgerConfig = new KafkaLedgerConfig().withMaxConsumerPipeYields(1);
    return createLedger(kafka, ledgerConfig, asyncProducer, asyncConsumer, pipelineSizeBatches, zlg);
  }

  private static KafkaLedger createLedger(Kafka<String, Message> kafka, 
                                          KafkaLedgerConfig baseConfig,
                                          boolean asyncProducer, 
                                          boolean asyncConsumer, 
                                          int pipelineSizeBatches, 
                                          Zlg zlg) {
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
    final var kafka = new MockKafka<String, Message>();
    ledger = createLedger(kafka, new KafkaLedgerConfig().withPrintConfig(true), 
                          false, true, 1, new MockLogTarget().logger());
    final var barrierA = new CyclicBarrier(2);
    final var barrierB = new CyclicBarrier(2);
    final var received = new AtomicInteger();
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
  public void testAppend_sendCallbackExceptionLoggerPass() {
    final var logTarget = new MockLogTarget();
    final var kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(ExceptionGenerator.never());
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0));
    assertEquals(0, logTarget.entries().forLevel(LogLevel.WARN).list().size());
  }

  @Test
  public void testAppend_sendCallbackExceptionLoggerFail() {
    final var logTarget = new MockLogTarget();
    final var exception = new Exception("simulated");
    final var kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(ExceptionGenerator.once(exception));
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());

    wait.until(() -> {
      logTarget.entries().forLevel(LogLevel.WARN).withThrowable(exception).assertCount(1);
    });
  }

  @Test
  public void testAppend_sendCallbackRetriableException() {
    final var logTarget = new MockLogTarget();
    final var exception = new CorruptRecordException("simulated");
    final var exGen = ExceptionGenerator.times(exception, 2);
    final var mockExGen = Classes.<ExceptionGenerator<ProducerRecord<String, Message>, Exception>>cast(mock(ExceptionGenerator.class));
    when(mockExGen.inspect(any())).thenAnswer(invocation -> exGen.inspect(invocation.getArgument(0)));

    final var kafka = new MockKafka<String, Message>()
        .withSendCallbackExceptionGenerator(mockExGen);
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());

    wait.until(() -> {
      logTarget.entries().forLevel(LogLevel.WARN).withThrowable(exception).assertCount(2);
      verify(mockExGen, times(3)).inspect(any());
    });
  }

  @Test
  public void testSend_runtimeException() {
    final var logTarget = new MockLogTarget();
    final var kafka = new MockKafka<String, Message>()
        .withSendRuntimeExceptionGenerator(ExceptionGenerator.once(new IllegalStateException("simulated")));
    ledger = createLedger(kafka, false, true, 10, logTarget.logger());
    ledger.append(new Proposal("B100", new String[0], null, 0), AppendCallback.nop());
    wait.until(() -> {
      logTarget.entries().forLevel(LogLevel.WARN).withThrowableType(ProducerException.class).assertCount(1);
    });
  }

  @Test
  public void testAttach_grouped_commitCallbackExceptionLogger() {
    final var logTarget = new MockLogTarget();
    final var exception = new Exception("simulated");
    final var kafka = new MockKafka<String, Message>()
        .withCommitExceptionGenerator(ExceptionGenerator.once(exception));
    final var baseConfig = new KafkaLedgerConfig().withDrainConfirmations(false);
    ledger = createLedger(kafka, baseConfig, false, true, 10, logTarget.logger());
    final var groupId = "test";

    final var received = new AtomicInteger();
    ledger.attach(new MessageHandler() {
      @Override
      public String getGroupId() {
        return groupId;
      }

      @Override
      public void onMessage(MessageContext context, Message message) {
        assertTrue(context.isAssigned(message));
        try {
          context.beginAndConfirm(message);
        } catch (Throwable e) {
          e.printStackTrace();
          fail("Unexpected exception: " + e);
        }
        received.incrementAndGet();
      }
    });

    ledger.append(new Proposal("B100", new String[0], null, 0));
    wait.until(() -> {
      assertEquals(1, received.get());
      logTarget.entries().forLevel(LogLevel.WARN).containing("Error committing offsets").withThrowable(exception).assertCount(1);
    });
  }

  @Test
  public void testAttach_grouped_receiveNormal() {
    final var logTarget = new MockLogTarget();
    final var groupId = "test";
    final var kafka = new MockKafka<String, Message>(1, 100);
    final var baseConfig = new KafkaLedgerConfig().withDrainConfirmations(true);
    ledger = createLedger(kafka, baseConfig, false, false, 10, logTarget.logger());

    final var received = new AtomicInteger();
    ledger.attach(new MessageHandler() {
      @Override
      public String getGroupId() {
        return groupId;
      }

      @Override
      public void onMessage(MessageContext context, Message message) {
        assertTrue(context.isAssigned(message));
        try {
          context.beginAndConfirm(message);
        } catch (Throwable e) {
          e.printStackTrace();
          fail("Unexpected exception: " + e);
        }
        received.incrementAndGet();
      }
    });

    ledger.append(new Proposal("B100", new String[0], null, 0));
    wait.until(() -> {
      assertEquals(1, received.get());
    });
  }

  @Test
  public void testAttach_ungrouped_receiveNormal() {
    final var logTarget = new MockLogTarget();
    final var kafka = new MockKafka<String, Message>(1, 100);
    final var baseConfig = new KafkaLedgerConfig().withDrainConfirmations(true);
    ledger = createLedger(kafka, baseConfig, false, false, 10, logTarget.logger());

    final var received = new AtomicInteger();
    ledger.attach(new NullGroupMessageHandler() {
      @Override
      public void onMessage(MessageContext context, Message message) {
        assertTrue(context.isAssigned(message));
        try {
          context.beginAndConfirm(message);
        } catch (Throwable e) {
          e.printStackTrace();
          fail("Unexpected exception: " + e);
        }
        received.incrementAndGet();
      }
    });

    ledger.append(new Proposal("B100", new String[0], null, 0));
    wait.until(() -> {
      assertEquals(1, received.get());
    });
  }

  @Test
  public void testAttach_afterDispose() {
    ledger = MockKafkaLedger.create();
    assertFalse(ledger.isDisposing());
    ledger.dispose();
    assertTrue(ledger.isDisposing());

    final var handler = mock(MessageHandler.class);
    ledger.attach(handler);

    ledger.append(new Proposal("B100", new String[0], null, 0));
    Threads.sleep(10);
    verifyNoMoreInteractions(handler);
  }

  @Test
  public void testAppend_afterDispose() {
    ledger = MockKafkaLedger.create();
    assertFalse(ledger.isDisposing());
    ledger.dispose();
    assertTrue(ledger.isDisposing());
    final var callback = mock(AppendCallback.class);
    ledger.append(new Proposal("B100", new String[0], null, 0), callback);
    Threads.sleep(10);
    verifyNoMoreInteractions(callback);
  }

  @Test
  public void testHandleRecord_withNoConsumerState() {
    final var handler = mock(MessageHandler.class);
    final var message = new Command("xid", null, 0);
    final var record = new ConsumerRecord<String, Message>("topic", 0, 0L, "key", message);
    final var logTarget = new MockLogTarget();

    KafkaLedger.handleRecord(handler, null, null, record, logTarget.logger());
    verify(handler).onMessage(isNull(), eq(message));
    logTarget.entries().assertCount(0);
  }

  @Test
  public void testHandleRecord_withAssignedTopic_enforceMonotonicity() {
    final var handler = mock(MessageHandler.class);
    final var consumerState = new ConsumerState(true);
    consumerState.assignedPartitions = Set.of(0);
    final var message = new Command("xid", null, 0);
    final var record = new ConsumerRecord<String, Message>("topic", 0, 0L, "key", message);
    final var logTarget = new MockLogTarget();

    KafkaLedger.handleRecord(handler, consumerState, null, record, logTarget.logger());
    verify(handler).onMessage(isNull(), eq(message));
    logTarget.entries().assertCount(0);
  }

  @Test
  public void testHandleRecord_withMonotonicConstraintBreach() {
    final var handler = mock(MessageHandler.class);
    final var consumerState = new ConsumerState(true);
    consumerState.assignedPartitions = Set.of(0);
    consumerState.offsetsProcessed.computeIfAbsent(0, __ -> new MutableOffset()).offset = 0;
    final var message = new Command("xid", null, 0);
    final var record = new ConsumerRecord<String, Message>("topic", 0, 0L, "key", message);
    final var logTarget = new MockLogTarget();

    KafkaLedger.handleRecord(handler, consumerState, null, record, logTarget.logger());
    verify(handler, never()).onMessage(isNull(), eq(message));
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG)
    .containing("Skipping message at offset 0, partition 0: monotonicity constraint breached (previous offset: 0)").assertCount(1);
  }

  @Test
  public void testHandleRecord_withUnassignedTopic() {
    final var handler = mock(MessageHandler.class);
    final var consumerState = new ConsumerState(false);
    final var message = new Command("xid", null, 0);
    final var record = new ConsumerRecord<String, Message>("topic", 0, 0L, "key", message);
    final var logTarget = new MockLogTarget();

    KafkaLedger.handleRecord(handler, consumerState, null, record, logTarget.logger());
    verify(handler, never()).onMessage(isNull(), eq(message));
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG)
    .containing("Skipping message at offset 0: partition 0 has been reassigned").assertCount(1);
  }

  @Test
  public void testConfirm_intermediate() {
    final var messageId = new DefaultMessageId(0, 100L);
    final var consumerState = new ConsumerState(false);
    consumerState.offsetsPending.put(0, 200L);
    final var logTarget = new MockLogTarget();

    KafkaLedger.confirm(messageId, consumerState, "topic", logTarget.logger());
    assertTrue(consumerState.offsetsConfirmed.containsKey(new TopicPartition("topic", 0)));
    assertTrue(consumerState.offsetsConfirmed.containsValue(new OffsetAndMetadata(101L)));
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.TRACE).containing("Confirmed intermediate 100 from 200 (partition 0)").assertCount(1);
  }

  @Test
  public void testConfirm_last() {
    final var messageId = new DefaultMessageId(0, 100L);
    final var consumerState = new ConsumerState(false);
    consumerState.offsetsPending.put(0, 100L);
    final var logTarget = new MockLogTarget();

    KafkaLedger.confirm(messageId, consumerState, "topic", logTarget.logger());
    assertTrue(consumerState.offsetsConfirmed.containsKey(new TopicPartition("topic", 0)));
    assertTrue(consumerState.offsetsConfirmed.containsValue(new OffsetAndMetadata(101L)));
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.TRACE).containing("Confirmed last 100 (partition 0)").assertCount(1);
  }

  @Test
  public void testCommitOffsets_withEnqueued_enforceMonotonic() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    doAnswer(invocation -> {
      final var callback = invocation.getArgument(1, OffsetCommitCallback.class);
      callback.onComplete(Map.of(), null);
      return null;
    }).when(consumer).commitAsync(any(), any());

    final var consumerState = new ConsumerState(true);
    final var logTarget = new MockLogTarget();
    final var topicPartition = new TopicPartition("topic", 0);
    final var offsetsAndMetadata = new OffsetAndMetadata(100L);
    consumerState.offsetsConfirmed.put(topicPartition, offsetsAndMetadata);
    final var offsetsConfirmed = consumerState.offsetsConfirmed;

    KafkaLedger.commitOffsets(consumer, consumerState, logTarget.logger());
    assertEquals(1, consumerState.offsetsProcessed.size());
    assertEquals(offsetsAndMetadata.offset() - 1, consumerState.offsetsProcessed.get(topicPartition.partition()).offset);
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.TRACE).containing("Committing offsets {0=100}").assertCount(1);
    verify(consumer).commitAsync(eq(offsetsConfirmed), isNotNull());
  }

  @Test
  public void testCommitOffsets_withEnqueued_nonMonotonic() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    doAnswer(invocation -> {
      final var callback = invocation.getArgument(1, OffsetCommitCallback.class);
      callback.onComplete(Map.of(), null);
      return null;
    }).when(consumer).commitAsync(any(), any());

    final var consumerState = new ConsumerState(false);
    final var logTarget = new MockLogTarget();
    final var topicPartition = new TopicPartition("topic", 0);
    final var offsetsAndMetadata = new OffsetAndMetadata(100L);
    consumerState.offsetsConfirmed.put(topicPartition, offsetsAndMetadata);
    final var offsetsConfirmed = consumerState.offsetsConfirmed;

    KafkaLedger.commitOffsets(consumer, consumerState, logTarget.logger());
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.TRACE).containing("Committing offsets {0=100}").assertCount(1);
    verify(consumer).commitAsync(eq(offsetsConfirmed), isNotNull());
  }

  @Test
  public void testCommitOffsets_withNoneEnqueued_enforceMonotonic() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    doAnswer(invocation -> {
      final var callback = invocation.getArgument(1, OffsetCommitCallback.class);
      callback.onComplete(Map.of(), null);
      return null;
    }).when(consumer).commitAsync(any(), any());
    final var consumerState = new ConsumerState(true);
    final var logTarget = new MockLogTarget();

    KafkaLedger.commitOffsets(consumer, consumerState, logTarget.logger());
    assertTrue(consumerState.offsetsProcessed.isEmpty());
    logTarget.entries().assertCount(0);
    verify(consumer, never()).commitAsync(any(), isNotNull());
  }

  @Test
  public void testCommitOffsets_withNoneEnqueued_nonMonotonic() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    doAnswer(invocation -> {
      final var callback = invocation.getArgument(1, OffsetCommitCallback.class);
      callback.onComplete(Map.of(), null);
      return null;
    }).when(consumer).commitAsync(any(), any());
    final var consumerState = new ConsumerState(false);
    assertNull(consumerState.offsetsProcessed);
    final var logTarget = new MockLogTarget();

    KafkaLedger.commitOffsets(consumer, consumerState, logTarget.logger());
    logTarget.entries().assertCount(0);
    verify(consumer, never()).commitAsync(any(), isNotNull());
  }

  @Test
  public void testSleepFor() {
    KafkaLedger.sleepFor(0).run();
  }

  @Test
  public void testDrainOffsets_withNoPendingOffsets() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    final var consumerState = new ConsumerState(false);
    final var logTarget = new MockLogTarget();
    final var intervalSleep = mock(Runnable.class);

    final var disposing = new AtomicBoolean();
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 10_000, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("All offsets confirmed: {}").assertCount(1);
    verify(consumer, never()).commitSync(Classes.<Map<TopicPartition, OffsetAndMetadata>>cast(any(Map.class)));
  }

  @Test
  public void testDrainOffsets_withPendingOffsets() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    final var consumerState = new ConsumerState(false);
    final var logTarget = new MockLogTarget();
    final var intervalSleep = mock(Runnable.class);
    consumerState.offsetsPending.put(0, 100L);
    consumerState.offsetsConfirmed.put(new TopicPartition("topic", 0), new OffsetAndMetadata(101L));
    doAnswer(__ -> {
      synchronized (consumerState.lock) {
        consumerState.offsetsPending.clear();
      }
      return null;
    }).when(intervalSleep).run();

    final var disposing = new AtomicBoolean();
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 10_000, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(2);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("All offsets confirmed: {0=100}").assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("Offsets pending: {0=100}").assertCount(1);
    final var expectedOffsets = Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(101L));
    verify(consumer).commitSync(eq(expectedOffsets));
  }

  @Test
  public void testDrainOffsets_withPendingOffsetsTimeout() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    final var consumerState = new ConsumerState(false);
    final var logTarget = new MockLogTarget();
    final var intervalSleep = mock(Runnable.class);
    consumerState.offsetsPending.put(0, 100L);

    final var disposing = new AtomicBoolean();
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 0, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(2);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("Offsets pending: {0=100}").assertCount(1);
    logTarget.entries().forLevel(LogLevel.WARN).containing("Drain timed out (0 ms). Pending offsets: {0:100}, 0 record(s) queued");
    verify(consumer, never()).commitSync(Classes.<Map<TopicPartition, OffsetAndMetadata>>cast(any(Map.class)));
  }

  @Test
  public void testDrainOffsets_whileDisposing() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    final var consumerState = new ConsumerState(false);
    final var logTarget = new MockLogTarget();
    final var intervalSleep = mock(Runnable.class);

    final var disposing = new AtomicBoolean(true);
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 10_000, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(0);
    verify(consumer, never()).commitSync(Classes.<Map<TopicPartition, OffsetAndMetadata>>cast(any(Map.class)));
  }

  @Test
  public void testDrainOffsets_withBackloggedPipeline() {
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    final var consumerState = new ConsumerState(false);
    final var logTarget = new MockLogTarget();
    final var intervalSleep = mock(Runnable.class);
    consumerState.queuedRecords = 1;
    consumerState.offsetsConfirmed.put(new TopicPartition("topic", 0), new OffsetAndMetadata(101L));
    doAnswer(__ -> {
      synchronized (consumerState.lock) {
        consumerState.queuedRecords = 0;
      }
      return null;
    }).when(intervalSleep).run();

    final var disposing = new AtomicBoolean();
    KafkaLedger.drainOffsets("topic", consumer, consumerState, 1, intervalSleep, 10_000, disposing::get, logTarget.logger());
    logTarget.entries().assertCount(2);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("All offsets confirmed: {0=100}").assertCount(1);
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("Pipeline backlogged: 1 record(s) queued").assertCount(1);
    final var expectedOffsets = Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(101L));
    verify(consumer).commitSync(eq(expectedOffsets));
  }

  @Test
  public void testQueueRecords_withConsumerStateAndAssignedPartitions() throws InterruptedException {
    final var consumerState = new ConsumerState(false);
    consumerState.assignedPartitions = Set.of(0);
    final var logTarget = new MockLogTarget();
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    final var recordHandler = Classes.<RecordHandler<String, Message>>cast(mock(RecordHandler.class));
    final var consumerPipe = new ConsumerPipe<>(new ConsumerPipeConfig().withAsync(false), recordHandler, null);
    final var records = getSingletonBatch();

    KafkaLedger.queueRecords(consumer, consumerState, consumerPipe, records, 1, logTarget.logger());
    verify(recordHandler).onReceive(isNotNull());
    assertEquals(1, consumerState.queuedRecords);
    logTarget.entries().assertCount(0);
  }

  @Test
  public void testQueueRecords_withConsumerStateAndNoAssignedPartitions() throws InterruptedException {
    final var consumerState = new ConsumerState(false);
    consumerState.assignedPartitions = Set.of();
    final var logTarget = new MockLogTarget();
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    final var recordHandler = Classes.<RecordHandler<String, Message>>cast(mock(RecordHandler.class));
    final var consumerPipe = new ConsumerPipe<>(new ConsumerPipeConfig().withAsync(false), recordHandler, null);
    final var records = getSingletonBatch();

    KafkaLedger.queueRecords(consumer, consumerState, consumerPipe, records, 1, logTarget.logger());
    verifyNoMoreInteractions(recordHandler);
    assertEquals(0, consumerState.queuedRecords);
    logTarget.entries().assertCount(0);
  }

  @Test
  public void testQueueRecords_withoutConsumerState() throws InterruptedException {
    final var logTarget = new MockLogTarget();
    final var consumer = Classes.<Consumer<String, Message>>cast(mock(Consumer.class));
    final var recordHandler = Classes.<RecordHandler<String, Message>>cast(mock(RecordHandler.class));
    final var consumerPipe = new ConsumerPipe<>(new ConsumerPipeConfig().withAsync(false), recordHandler, null);
    final var records = getSingletonBatch();

    KafkaLedger.queueRecords(consumer, null, consumerPipe, records, 1, logTarget.logger());
    verify(recordHandler).onReceive(isNotNull());
    logTarget.entries().assertCount(0);
  }

  @Test
  public void testMutableOffset_tryAdvance() {
    final var mutableOffset = new MutableOffset();
    assertTrue(mutableOffset.tryAdvance(0));
    assertEquals(0, mutableOffset.offset);

    assertFalse(mutableOffset.tryAdvance(0));
    assertEquals(0, mutableOffset.offset);
  }

  private static ConsumerRecords<String, Message> getSingletonBatch() {
    final var key = "key";
    final var value = (Message) null;
    return new ConsumerRecords<>(Map.of(new TopicPartition("topic", 0), 
                                        List.of(new ConsumerRecord<>("topic", 0, 0L, key, value))));
  }
}
