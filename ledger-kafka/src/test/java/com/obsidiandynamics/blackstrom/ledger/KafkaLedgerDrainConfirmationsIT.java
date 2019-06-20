package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

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
  public void testMultipleConsumersWithMultiplePartitions() {
    test("MultiConsMultiPart", 16, 400, 10, 4, 1_000, 100);
  }
  
  /**
   *  Verifies that messages can be processed exactly once in the presence of multiple consumers
   *  contending for a single group ID. Message flow should transition from one consumer to the
   *  next as consumers are added; the prior consumer relinquishing partition assignments will
   *  drain any pending confirmations before yielding to the new assignee. Messages may still
   *  be processed twice at the handover boundary; this is just how Kafka works — the next
   *  assignee will resume processing from the last confirmed offset, even if it means processing
   *  the message twice. (The test accounts for this, by allowing a limited number of duplicates,
   *  in line with the number of consumers.)
   *  
   *  @param testLabel How to label this test.
   *  @param partitions The number of partitions.
   *  @param messages The number of messages (per partition).
   *  @param messageIntervalMillis Interval between sending each group of messages (one message per partition).
   *  @param consumers The number of consumers.
   *  @param consumerJoinIntervalMillis Interval between consumers joining.
   *  @param commitDelayMillis Delay between receiving a message and committing the offset.
   */
  private void test(String testLabel,
                    int partitions,
                    int messages,
                    int messageIntervalMillis,
                    int consumers,
                    int consumerJoinIntervalMillis,
                    int commitDelayMillis) {
    zlg.i("Starting test %s", z -> z.arg(testLabel));
    
    final var topicName = Exceptions.wrap(() -> createTopic(testLabel, partitions), RuntimeException::new);
    
    ledger = new KafkaLedger(new KafkaLedgerConfig()
                             .withKafka(new KafkaCluster<>(config))
                             .withTopic(topicName)
                             .withDrainConfirmations(true)
                             .withDrainConfirmationsTimeout(Integer.MAX_VALUE)
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
    // creating the next consumer. Ensures that one consumer (and its peers) have rebalanced before
    // adding another consumer into the mix.
    final var maxPreviousConsumerReceiveWaitMillis = 30_000;
    
    // The minimum an received offset must move by before adding a new consumer. This value shouldn't be smaller
    // than two, as it guarantees a clean boundary between consumer rebalancing.
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
            
            receivedPerConsumer.incrementAndGet(consumerIndex);
            final var receiveCounts = receiveCountsPerPartition[partition];
            final var count = receiveCounts.incrementAndGet((int) offset);
            
            // log any message that has been processed more than once (unless it is a one-off)
            final var lastCount = offset == 0 ? 1 : receiveCounts.get((int) offset - 1);
            if (lastCount == 0) {
              zlg.w("FAILURE in message %s; count=%d, lastCount=%d", z -> z.arg(message::getMessageId).arg(count).arg(lastCount));
              System.err.format("FAILURE in message %s; count=%d, lastCount=%d\n", message.getMessageId(), count, lastCount);
            } if (count > 1) {
              zlg.d("Seeing boundary duplicate: offset=%d, partition=%d, count=%d", z -> z.arg(offset).arg(partition).arg(count));
            }
            totalReceived.incrementAndGet();
            
            // delay offset commits for a more pronounced effect (increases likelihood of repeated messages
            // after rebalancing)
            final var confirmNoSoonerThan = System.currentTimeMillis() + commitDelayMillis;
            try {
              executor.submit(() -> {
                final var sleepNeeded = confirmNoSoonerThan - System.currentTimeMillis();
                Threads.sleep(sleepNeeded);
                confirmation.confirm();
                totalConfirmed.incrementAndGet();
              });
            } catch (RejectedExecutionException e) {
              zlg.i("Executor terminated: confirming offset %d (partition %d) in handler thread", z -> z.arg(offset).arg(partition));
              confirmation.confirm();
              totalConfirmed.incrementAndGet();
            }
          }
        });
        
        // wait for at least one message to be received before adding more consumers into the mix
        // (prevents adding contending consumers before at least one has been assigned a partition)
        Wait.LONG.untilTrue(() -> totalReceived.get() >= 1);
      }
    }, "LedgerAttachThread").start();

    zlg.i("Starting publisher");
    for (var m = 0; m < messages; m++) {
      for (var p = 0; p < partitions; p++) {
        ledger.append(new Command(p + "-" + m, null, 0).withShard(p));
      }
      Threads.sleep(messageIntervalMillis);

      // wait for at least one message to be received before publishing more
      // (prevents over-publishing before a consumer has been assigned a partition)
      Wait.MEDIUM.untilTrue(() -> totalReceived.get() >= 1);
    }
    
    // await receival of all messages
    zlg.i("Awaiting message receival");
    final var expectedMessages = messages * partitions;
    var allReceived = false;
    try {
      Wait.LONG.until(() -> {
        final var uniqueCount = getUniqueCount(receiveCountsPerPartition);
        assertEquals(expectedMessages, uniqueCount);
      });
      allReceived = true;
    } finally {
      if (! allReceived) {
        zlg.w("Test failed");
        for (var p = 0; p < partitions; p++) {
          final var receiveCounts = receiveCountsPerPartition[p];
          for (var offset = 0; offset < receiveCounts.length(); offset++) {
            final var receiveCount = receiveCounts.get(offset);
            if (receiveCount == 0) {
              final var _offset = offset;
              final var _p = p;
              zlg.w("Missing offset %d for partition %d", z -> z.arg(_offset).arg(_p));
              System.err.format("Missing offset %d for partition %d\n", offset, p);
            }
          }
        }
      }
    }
    
    final var errors = new ArrayList<>();
    
    // This number is more of a heuristic. In an ideal run, the number of expected duplicates equals 
    // the number of rebalances (one less than the number of consumers). Occasionally rebalances
    // will occur more often (in-between consumer additions), especially when the brokers and the consumers
    // are heavily loaded. So we allow for a generous grace of 3 times the expected number (which will
    // still easily trap genuine bugs).
    final var allowedDuplicates = (consumers - 1) * 3;
    for (var p = 0; p < partitions; p++) {
      final var receiveCounts = receiveCountsPerPartition[p];
      final var duplicates = getDuplicatesCount(receiveCounts);
      if (duplicates > allowedDuplicates) {
        final var errorMessage = String.format("partition: %d, duplicates %d", p, duplicates);
        errors.add(errorMessage);
        System.err.println(errorMessage);
        for (var offset = 0; offset < receiveCounts.length(); offset++) {
          System.err.format("  offset: %d, count: %d\n", offset, receiveCounts.get(offset));
        }
      }
    }
    
    assertEquals(Collections.emptyList(), errors);

    ledger.dispose();
    
    zlg.i("Awaiting final confirmations");
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() >= getUniqueCount(receiveCountsPerPartition));
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() == totalConfirmed.get());
    zlg.i("Test passed: %s", z -> z.arg(testLabel));
  }
  
  private static int getDuplicatesCount(AtomicIntegerArray receiveCounts) {
    var duplicates = 0;
    for (var offset = 0; offset < receiveCounts.length(); offset++) {
      if (receiveCounts.get(offset) > 1) {
        duplicates++;
      }
    }
    return duplicates;
  }
  
  private static int getUniqueCount(AtomicIntegerArray[] receiveCountsPerPartition) {
    var totalUnique = 0;
    for (var receiveCounts : receiveCountsPerPartition) {
      for (var offset = 0; offset < receiveCounts.length(); offset++) {
        if (receiveCounts.get(offset) != 0) {
          totalUnique++;
        }
      }
    }
    return totalUnique;
  }

  private static int[] getLatestOffsets(AtomicIntegerArray[] receiveCountsPerPartition) {
    final var latestOffsets = new int[receiveCountsPerPartition.length];
    for (var p = 0; p < receiveCountsPerPartition.length; p++) {
      latestOffsets[p] = getLatestOffset(receiveCountsPerPartition[p]);
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
    for (var p = 0; p < offsetsBefore.length; p++) {
      if (offsetsAfter[p] - offsetsBefore[p] < minMagnitude) {
        return false;
      }
    }
    return true;
  }
}
