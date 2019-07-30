package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.assertj.core.api.*;
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
import com.obsidiandynamics.retry.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

@RunWith(Parameterized.class)
public final class KafkaLedgerDrainConfirmationsIT {
  private static final Timesert WAIT = Wait.LONG;
  
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }

  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TestKafka.start();
  }
  
  private final KafkaClusterConfig config = new KafkaClusterConfig()
      .withBootstrapServers(TestKafka.bootstrapServers())
      .withProducerProps(new PropsBuilder()
                         .build())
      .withConsumerProps(new PropsBuilder()
                         .with("default.api.timeout.ms", 5_000)
                         .with("session.timeout.ms", 60_000)
                         .with("max.poll.interval.ms", 300_000)
                         .with("partition.assignment.strategy", RoundRobinAssignor.class.getName())
                         .build());
  
  private ExecutorService executor;
  
  private KafkaLedger ledger;
  
  private final String timestamp = new SimpleDateFormat(KafkaDefaults.TOPIC_DATE_FORMAT).format(new Date());

  @After
  public void after() {
    if (executor != null) {
      executor.shutdown();
    }
    
    if (ledger != null) {
      ledger.dispose();
    }
  }
  
  private static String getUniquePrefix(String label) {
    return KafkaLedgerDrainConfirmationsIT.class.getSimpleName() + "-" + label;
  }
  
  private String getUniqueName(String label) {
    return getUniquePrefix(label) + "-" + timestamp;
  }
  
  private String createTopic(String label, int partitions) throws Exception {
    try (var admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaDefaults.CLUSTER_AWAIT);
      
      final var uniquePrefix = getUniquePrefix(label);
      final var allTopics = admin.listTopics(KafkaDefaults.TOPIC_OPERATION);
      final var testTopics = allTopics.stream().filter(topic -> topic.startsWith(uniquePrefix)).collect(Collectors.toList());

      new Retry()
      .withAttempts(10)
      .withBackoff(10_000)
      .withFaultHandler(zlg::w)
      .withErrorHandler(zlg::e)
      .withExceptionMatcher(Retry.isA(TimeoutException.class))
      .run(() -> admin.deleteTopics(testTopics, KafkaDefaults.TOPIC_OPERATION));

      final var allGroups = admin.listConsumerGroups(KafkaDefaults.CONSUMER_GROUP_OPERATION);
      final var testGroups = allGroups.stream().filter(group -> group.startsWith(uniquePrefix)).collect(Collectors.toList());
      zlg.d("Deleting groups %s", z -> z.arg(testGroups));
      
      new Retry()
      .withAttempts(10)
      .withBackoff(10_000)
      .withFaultHandler(zlg::w)
      .withErrorHandler(zlg::e)
      .withExceptionMatcher(Retry.isA(TimeoutException.class))
      .run(() -> admin.deleteConsumerGroups(testGroups, KafkaDefaults.CONSUMER_GROUP_OPERATION));
      
      final var topicName = getUniqueName(label);
      TestTopic.createTopics(admin, TestTopic.newOf(topicName, partitions));
      return topicName;
    }
  }
  
  @Test
  public void testMultipleConsumersWithMultiplePartitions_enforceMonotonicity() {
    test("MultiConsMultiPart_mono", 16, 400, 10, 4, 1_000, 100, true);
  }
  
  @Test
  public void testMultipleConsumersWithMultiplePartitions_nonMonotonic() {
    test("MultiConsMultiPart_nonMono", 16, 400, 10, 4, 1_000, 100, false);
  }
  
  /**
   *  Verifies that messages can be processed exactly once in the presence of multiple consumers
   *  contending for a single group ID. Message flow should transition from one consumer to the
   *  next as consumers are added; the prior consumer relinquishing partition assignments will
   *  drain any pending confirmations before yielding to the new assignee.
   *  
   *  @param testLabel How to label this test.
   *  @param partitions The number of partitions.
   *  @param messages The number of messages (per partition).
   *  @param messageIntervalMillis Interval between sending each group of messages (one message per partition).
   *  @param consumers The number of consumers.
   *  @param consumerJoinIntervalMillis Interval between consumers joining.
   *  @param commitDelayMillis Delay between receiving a message and committing the offset.
   *  @param enforceMonotonicity Whether the monotonicity constraint should be enforced.
   */
  private void test(String testLabel,
                    int partitions,
                    int messages,
                    int messageIntervalMillis,
                    int consumers,
                    int consumerJoinIntervalMillis,
                    int commitDelayMillis,
                    boolean enforceMonotonicity) {
    zlg.i("Starting test %s", z -> z.arg(testLabel));
    
    final var topicName = Exceptions.wrap(() -> createTopic(testLabel, partitions), RuntimeException::new);
    
    ledger = new KafkaLedger(new KafkaLedgerConfig()
                             .withKafka(new KafkaCluster<>(config))
                             .withTopic(topicName)
                             .withProducerPipeConfig(new ProducerPipeConfig().withAsync(true))
                             .withConsumerPipeConfig(new ConsumerPipeConfig().withAsync(true))
                             .withDrainConfirmations(true)
                             .withDrainConfirmationsTimeout(Integer.MAX_VALUE)
                             .withEnforceMonotonicity(enforceMonotonicity)
                             .withZlg(zlg)
                             .withCodec(new KryoMessageCodec(true)));
    ledger.init();
    
    final var receiveCountsPerPartition = new AtomicIntegerArray[partitions];
    for (var p = 0; p < partitions; p++) {
      receiveCountsPerPartition[p] = new AtomicIntegerArray(messages);
    }
    final var totalReceived = new AtomicInteger();
    final var totalConfirmed = new AtomicInteger();
    final var receivedPerConsumer = new AtomicIntegerArray(consumers);
    executor = Executors.newFixedThreadPool(8);
    
    // The time to wait for the previous consumer to have received at least one message before
    // spawning the next consumer. Ensures that one consumer (and its peers) have rebalanced before
    // adding another consumer into the mix.
    final var maxPreviousConsumerReceiveWaitMillis = 30_000;
    
    // The minimum a received offset must move by before adding a new consumer.
    final var minOffsetMove = 10;
    final var maxOffsetMoveWaitMillis = 30_000;
    
    final var groupId = getUniqueName(testLabel);
    new Thread(() -> {
      var latestOffsetsFromLastRebalance = getLatestOffsets(receiveCountsPerPartition);
      for (var c = 0; c < consumers; c++) {
        final var consumerIndex = c;
        if (c != 0) {
          Threads.sleep(consumerJoinIntervalMillis);
          final var previousConsumerReceiveWaitUntilTime = System.currentTimeMillis() + maxPreviousConsumerReceiveWaitMillis;
          for (;;) {
            final var previousCounsumerReceived = receivedPerConsumer.get(c - 1);
            if (previousCounsumerReceived > 0) {
              break;
            } else {
              zlg.d("Previous consumer (at index %d) hasn't yet received a single message", z -> z.arg(consumerIndex - 1));
              if (System.currentTimeMillis() > previousConsumerReceiveWaitUntilTime) {
                zlg.d("Previous consumer seems to have stopped receiving messages; no more consumers will be provisioned");
                return;
              } else {
                Threads.sleep(50);
              }
            }
          }
          
          final var offsetMoveWaitUntilTime = System.currentTimeMillis() + maxOffsetMoveWaitMillis;
          for (;;) {
            final var latestOffsets = getLatestOffsets(receiveCountsPerPartition);
            if (offsetsMoved(latestOffsetsFromLastRebalance, latestOffsets, minOffsetMove)) {
              latestOffsetsFromLastRebalance = latestOffsets;
              break;
            } else {
              zlg.d("Offsets haven't moved significantly since last rebalance: %s",
                    z -> z.arg(Arrays.toString(latestOffsets)));
              if (System.currentTimeMillis() > offsetMoveWaitUntilTime) {
                zlg.d("Offsets don't appear to be moving; no more consumers will be provisioned");
                return;
              } else {
                Threads.sleep(50);
              }
            }
          }
        }

        zlg.i("Attaching consumer");
        ledger.attach(new MessageHandler() {
          @Override
          public String getGroupId() {
            return groupId;
          }
    
          @Override
          public void onMessage(MessageContext context, Message message) {
            final var confirmation = context.begin(message);
            final var partition = ((DefaultMessageId) message.getMessageId()).getShard();
            final var offset = ((DefaultMessageId) message.getMessageId()).getOffset();
            
            if (offset == messages - 1) {
              zlg.d("Received last message at offset %d (partition %d)", z -> z.arg(offset).arg(partition));
            }
            
            final boolean accepted;
            if (accepted = offset < messages) {
              receivedPerConsumer.incrementAndGet(consumerIndex);
              final var receiveCounts = receiveCountsPerPartition[partition];
              final var count = receiveCounts.incrementAndGet((int) offset);
              
              // log any message that has been processed more than once or if a preceding message has been skipped
              final var lastCount = offset == 0 ? 1 : receiveCounts.get((int) offset - 1);
              if (lastCount == 0) {
                zlg.w("FAILURE in message %s; out-of-order: count=%d, lastCount=%d", 
                      z -> z.arg(message::getMessageId).arg(count).arg(lastCount));
              } else if (count > 1) {
                zlg.w("FAILURE in message %s; duplicate: offset=%d, partition=%d, count=%d", 
                      z -> z.arg(message::getMessageId).arg(offset).arg(partition).arg(count));
              }
              totalReceived.incrementAndGet();
            } else {
              // extraneous messages are possible (albeit rare), occurring if the producer encountered a retriable
              // error during publishing and re-attempted to publish, although the initial publish had succeeded
              zlg.d("Received extraneous messsage %s", z -> z.arg(message::getMessageId));
            }
            
            // delay offset commits for a more pronounced effect (increases likelihood of repeated messages
            // after rebalancing if drainage is not working correctly)
            final var confirmNoSoonerThan = System.currentTimeMillis() + commitDelayMillis;
            try {
              executor.submit(() -> {
                final var sleepNeeded = confirmNoSoonerThan - System.currentTimeMillis();
                Threads.sleep(sleepNeeded);
                confirmation.confirm();
                if (accepted) totalConfirmed.incrementAndGet();
              });
            } catch (RejectedExecutionException e) {
              zlg.i("Executor terminated: confirming offset %d (partition %d) in handler thread", z -> z.arg(offset).arg(partition));
              confirmation.confirm();
              if (accepted) totalConfirmed.incrementAndGet();
            }
          }
        });
        
        // wait for at least one message to be received before adding more consumers into the mix
        // (prevents adding contending consumers before at least one has been assigned a partition)
        WAIT.untilTrue(() -> totalReceived.get() >= 1);
      }
    }, "LedgerAttachThread").start();

    zlg.i("Starting publisher");
    for (var m = 0; m < messages + 100; m++) {
      for (var p = 0; p < partitions; p++) {
        ledger.append(new Command(p + "-" + m, null, 0).withShard(p));
      }
      Threads.sleep(messageIntervalMillis);

      // wait for at least one message to be received before publishing more
      // (prevents over-publishing before a consumer has been assigned a partition)
      WAIT.untilTrue(() -> totalReceived.get() >= 1);
    }
    
    // await receival of all messages
    zlg.i("Awaiting message receival");
    final var expectedMessages = messages * partitions;
    final var errors = new ArrayList<>();
    try {
      WAIT.until(() -> {
        errors.clear();
        final var receiveCountsPerPartitionCopy = copy(receiveCountsPerPartition);
        final var receivedMessages = countAll(receiveCountsPerPartitionCopy);
        if (expectedMessages != receivedMessages) {
          for (var p = 0; p < partitions; p++) {
            final var receiveCounts = receiveCountsPerPartitionCopy[p];
            for (var offset = 0; offset < receiveCounts.length; offset++) {
              if (receiveCounts[offset] != 1) {
                errors.add(String.format("Seeing %,d messages for %d#%d", receiveCounts[offset], p, offset));
              }
            }
          }
          
          fail(String.format("Expected %,d messages; got %,d", expectedMessages, receivedMessages));
        }
      });
    } finally {
      if (! errors.isEmpty()) {
        for (var error : errors) {
          zlg.w(error);
        }
      }
    }

    ledger.dispose();
    
    zlg.i("Awaiting final confirmations");
    WAIT.until(() -> {
      assertEquals(totalReceived.get(), totalConfirmed.get());
    });
    zlg.i("Test passed: %s", z -> z.arg(testLabel));
  }
  
  private static int[][] copy(AtomicIntegerArray[] receiveCountsPerPartition) {
    final var copy = new int[receiveCountsPerPartition.length][];
    for (var partition = 0; partition < copy.length; partition++) {
      final var src = receiveCountsPerPartition[partition];
      final var tgt = new int[src.length()];
      copy[partition] = tgt;
      for (var offset = 0; offset < tgt.length; offset++) {
        tgt[offset] = src.get(offset);
      }
    }
    return copy;
  }
  
  private static int countAll(int[][] receiveCountsPerPartition) {
    var receivedCount = 0;
    for (var partition = 0; partition < receiveCountsPerPartition.length; partition++) {
      for (var offset = 0; offset < receiveCountsPerPartition[partition].length; offset++) {
        receivedCount += receiveCountsPerPartition[partition][offset];
      }
    }
    return receivedCount;
  }
  
  private static int[] getLatestOffsets(AtomicIntegerArray[] receiveCountsPerPartition) {
    final var latestOffsets = new int[receiveCountsPerPartition.length];
    for (var partition = 0; partition < receiveCountsPerPartition.length; partition++) {
      latestOffsets[partition] = getLatestOffset(receiveCountsPerPartition[partition]);
    }
    return latestOffsets;
  }
  
  private static int getLatestOffset(AtomicIntegerArray receiveCounts) {
    for (var offset = receiveCounts.length(); --offset >= 0;) {
      if (receiveCounts.get(offset) != 0) {
        return offset;
      }
    }
    return -1;
  }
  
  private static boolean offsetsMoved(int[] offsetsBefore, int[] offsetsAfter, int minMagnitude) {
    for (var partition = 0; partition < offsetsBefore.length; partition++) {
      if (offsetsAfter[partition] - offsetsBefore[partition] < minMagnitude) {
        return false;
      }
    }
    return true;
  }
}
