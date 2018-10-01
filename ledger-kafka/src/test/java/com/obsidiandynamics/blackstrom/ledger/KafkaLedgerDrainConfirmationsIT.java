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
import com.obsidiandynamics.flow.*;
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
    new KafkaDocker().withComposeFile("stack/docker-compose.yaml").start();
  }
  
  private final KafkaClusterConfig config = new KafkaClusterConfig()
      .withBootstrapServers("localhost:9092")
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
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaDefaults.CLUSTER_AWAIT);
      
      final String uniquePrefix = getUniquePrefix(label);
      final Set<String> allTopics = admin.listTopics(KafkaDefaults.TOPIC_OPERATION);
      final List<String> testTopics = allTopics.stream().filter(topic -> topic.startsWith(uniquePrefix)).collect(Collectors.toList());

      new Retry()
      .withAttempts(10)
      .withBackoff(10_000)
      .withFaultHandler(zlg::w)
      .withErrorHandler(zlg::e)
      .withExceptionMatcher(Retry.isA(TimeoutException.class))
      .run(() -> admin.deleteTopics(testTopics, KafkaDefaults.TOPIC_OPERATION));

      final Set<String> allGroups = admin.listConsumerGroups(KafkaDefaults.CONSUMER_GROUP_OPERATION);
      final List<String> testGroups = allGroups.stream().filter(group -> group.startsWith(uniquePrefix)).collect(Collectors.toList());
      zlg.d("Deleting groups %s", z -> z.arg(testGroups));
      
      new Retry()
      .withAttempts(10)
      .withBackoff(10_000)
      .withFaultHandler(zlg::w)
      .withErrorHandler(zlg::e)
      .withExceptionMatcher(Retry.isA(TimeoutException.class))
      .run(() -> admin.deleteConsumerGroups(testGroups, KafkaDefaults.CONSUMER_GROUP_OPERATION));
      
      final String topicName = getUniqueName(label);
      admin.createTopics(Collections.singleton(new NewTopic(topicName, partitions, (short) 1)), KafkaDefaults.TOPIC_OPERATION);
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
   *  the message twice. (The test accounts for this, by allowing one-off messages to have two
   *  receivals, providing that these don't occur in succession.)
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
    
    final String topicName = Exceptions.wrap(() -> createTopic(testLabel, partitions), RuntimeException::new);
    
    ledger = new KafkaLedger(new KafkaLedgerConfig()
                             .withKafka(new KafkaCluster<>(config))
                             .withTopic(topicName)
                             .withDrainConfirmations(true)
                             .withDrainConfirmationsTimeout(Integer.MAX_VALUE)
                             .withZlg(zlg)
                             .withCodec(new KryoMessageCodec(true)));
    ledger.init();
    
    final AtomicIntegerArray[] receiveCountsPerPartition = new AtomicIntegerArray[partitions];
    for (int p = 0; p < partitions; p++) {
      receiveCountsPerPartition[p] = new AtomicIntegerArray(messages);
    }
    final AtomicInteger totalReceived = new AtomicInteger();
    final AtomicInteger totalConfirmed = new AtomicInteger();
    final AtomicIntegerArray receivedPerConsumer = new AtomicIntegerArray(consumers);
    executor = Executors.newFixedThreadPool(8);
    
    // The time to wait for the previous consumer to have received at least one message before
    // creating the next consumer. Ensures that one consumer (and its peers) have rebalanced before
    // adding another consumer into the mix.
    final int maxPreviousConsumerReceiveWaitMillis = 30_000;
    
    // The minimum an received offset must move by before adding a new consumer. This value shouldn't be smaller
    // than two, as it guarantees a clean boundary between consumer rebalancing.
    final int minOffsetMove = 10;
    final int maxOffsetMoveWaitMillis = 30_000;
    
    final String groupId = getUniqueName(testLabel);
    new Thread(() -> {
      int[] latestOffsetsFromLastRebalance = getLatestOffsets(receiveCountsPerPartition);
      for (int c = 0; c < consumers; c++) {
        final int consumerIndex = c;
        if (c != 0) {
          Threads.sleep(consumerJoinIntervalMillis);
          final long previousConsumerReceiveWaitUntilTime = System.currentTimeMillis() + maxPreviousConsumerReceiveWaitMillis;
          for (;;) {
            final int previousCounsumerReceived = receivedPerConsumer.get(c - 1);
            if (previousCounsumerReceived > 0) {
              break;
            } else {
              zlg.d("Previous consumer (at index %d) hasn't yet received a single message", z -> z.arg(consumerIndex - 1));
              if (System.currentTimeMillis() > previousConsumerReceiveWaitUntilTime) {
                zlg.d("Previous consumer seems to have stopped receiving messages; no more consumers will be provisioned");
                return;
              } else {
                Threads.sleep(10);
              }
            }
          }
          
          final long offsetMoveWaitUntilTime = System.currentTimeMillis() + maxOffsetMoveWaitMillis;
          for (;;) {
            final int[] latestOffsets = getLatestOffsets(receiveCountsPerPartition);
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
                Threads.sleep(10);
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
            final Confirmation confirmation = context.begin(message);
            final int partition = ((DefaultMessageId) message.getMessageId()).getShard();
            final long offset = ((DefaultMessageId) message.getMessageId()).getOffset();
            
            if (offset == messages - 1) {
              zlg.d("Received last message at offset %d (partition %d)", z -> z.arg(offset).arg(partition));
            }
            
            receivedPerConsumer.incrementAndGet(consumerIndex);
            final AtomicIntegerArray receiveCounts = receiveCountsPerPartition[partition];
            final int count = receiveCounts.incrementAndGet((int) offset);
            
            // log any message that has been processed more than once (unless it is a one-off)
            final int lastCount = offset == 0 ? 1 : receiveCounts.get((int) offset - 1);
            if (lastCount == 0) {
              zlg.w("FAILURE in message %s; count=%d, lastCount=%d", z -> z.arg(message::getMessageId).arg(count).arg(lastCount));
              System.err.format("FAILURE in message %s; count=%d, lastCount=%d\n", message.getMessageId(), count, lastCount);
            } else if (lastCount > 1 && count > 1) {
              zlg.w("FAILURE in message %s; count=%d, lastCount=%d", z -> z.arg(message::getMessageId).arg(count).arg(lastCount));
              System.err.format("FAILURE in message %s; count=%d, lastCount=%d\n", message.getMessageId(), count, lastCount);
            } else if (count > 1) {
              zlg.d("Seeing boundary duplicate: offset=%d, partition=%d, count=%d", z -> z.arg(offset).arg(partition).arg(count));
            }
            totalReceived.incrementAndGet();
            
            // delay offset commits for a more pronounced effect (increases likelihood of repeated messages
            // after rebalancing)
            final long confirmNoSoonerThan = System.currentTimeMillis() + commitDelayMillis;
            try {
              executor.submit(() -> {
                final long sleepNeeded = confirmNoSoonerThan - System.currentTimeMillis();
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
        Wait.MEDIUM.untilTrue(() -> totalReceived.get() >= 1);
      }
    }, "LedgerAttachThread").start();

    zlg.i("Starting publisher");
    for (int m = 0; m < messages; m++) {
      for (int p = 0; p < partitions; p++) {
        ledger.append(new Command(p + "-" + m, null, 0).withShard(p));
      }
      Threads.sleep(messageIntervalMillis);

      // wait for at least one message to be received before publishing more
      // (prevents over-publishing before at a consumer has been assigned a partition)
      Wait.MEDIUM.untilTrue(() -> totalReceived.get() >= 1);
    }
    
    // await receival of all messages
    final int expectedMessages = messages * partitions;
    boolean allReceived = false;
    try {
      Wait.MEDIUM.until(() -> {
        final int uniqueCount = getUniqueCount(receiveCountsPerPartition);
        assertEquals(expectedMessages, uniqueCount);
      });
      allReceived = true;
    } finally {
      if (! allReceived) {
        for (int p = 0; p < partitions; p++) {
          final AtomicIntegerArray receiveCounts = receiveCountsPerPartition[p];
          for (int offset = 0; offset < receiveCounts.length(); offset++) {
            final int receiveCount = receiveCounts.get(offset);
            if (receiveCount == 0) {
              System.err.format("Missing offset %d for partition %d\n", offset, p);
            }
          }
        }
      }
    }
    
    boolean passedOverall = true;
    final List<String> errors = new ArrayList<>();
    for (int p = 0; p < partitions; p++) {
      final AtomicIntegerArray receiveCounts = receiveCountsPerPartition[p];
      // the counts must all be 1; we only allow a spike in the count for one-off messages, signifying
      // an overlap between consumer rebalancing (which is a Kafka thing)
      int lastReceiveCount = 1;
      boolean passedForCurrentPartition = true;
      try {
        for (int offset = 0; offset < receiveCounts.length(); offset++) {
          final int receiveCount = receiveCounts.get(offset);
          if (lastReceiveCount > 1) {
            if (receiveCount != 1) {
              passedForCurrentPartition = false;
              errors.add("offset=" + offset + ", partition=" + p + ", receiveCount=" + receiveCount);
            }
          } else {
            if (receiveCount <= 0) {
              passedForCurrentPartition = false;
              errors.add("offset=" + offset + ", partition=" + p + ", receiveCount=" + receiveCount);
            }
          }
          lastReceiveCount = receiveCount;
        }
      } finally {
        if (! passedForCurrentPartition) {
          passedOverall = false;
          System.err.println("partition: " + p);
          for (int offset = 0; offset < receiveCounts.length(); offset++) {
            System.err.println("  offset: " + offset + ", count: " + receiveCounts.get(offset));
          }
        }
      }
    }
    
    assertTrue("errors=" + errors, passedOverall);

    ledger.dispose();
    
    zlg.i("Awaiting final confirmations");
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() >= getUniqueCount(receiveCountsPerPartition));
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() == totalConfirmed.get());
    zlg.i("Test passed: %s", z -> z.arg(testLabel));
  }
  
  private static int getUniqueCount(AtomicIntegerArray[] receiveCountsPerPartition) {
    int totalUnique = 0;
    for (AtomicIntegerArray receiveCounts : receiveCountsPerPartition) {
      for (int offset = 0; offset < receiveCounts.length(); offset++) {
        if (receiveCounts.get(offset) != 0) {
          totalUnique++;
        }
      }
    }
    return totalUnique;
  }

  private static int[] getLatestOffsets(AtomicIntegerArray[] receiveCountsPerPartition) {
    final int[] latestOffsets = new int[receiveCountsPerPartition.length];
    for (int p = 0; p < receiveCountsPerPartition.length; p++) {
      latestOffsets[p] = getLatestOffset(receiveCountsPerPartition[p]);
    }
    return latestOffsets;
  }
  
  private static int getLatestOffset(AtomicIntegerArray receiveCounts) {
    for (int offset = receiveCounts.length(); --offset >= 0;) {
      if (receiveCounts.get(offset) != 0) {
        return offset;
      }
    }
    return -1;
  }
  
  private static boolean offsetsMoved(int[] offsetsBefore, int[] offsetsAfter, int minMagnitude) {
    for (int p = 0; p < offsetsBefore.length; p++) {
      if (offsetsAfter[p] - offsetsBefore[p] < minMagnitude) {
        return false;
      }
    }
    return true;
  }
}
