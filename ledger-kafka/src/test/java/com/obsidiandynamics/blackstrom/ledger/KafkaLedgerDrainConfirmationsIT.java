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
import com.obsidiandynamics.threads.*;
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
      .withConsumerProps(new MapBuilder<Object, Object>()
                         .with("default.api.timeout.ms", "5000")
                         .with("session.timeout.ms", "60000")
                         .with("partition.assignment.strategy", RoundRobinAssignor.class.getName())
                         .build());
  
  private ExecutorService executor;
  
  private KafkaLedger ledger;
  
  @After
  public void after() {
    if (executor != null) {
      executor.shutdown();
    }
    
    if (ledger != null) {
      ledger.dispose();
    }
  }
  
  private String createTopic(String label, int partitions) throws InterruptedException, ExecutionException, TimeoutException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaDefaults.CLUSTER_AWAIT);
      
      final String topicPrefix = KafkaLedgerDrainConfirmationsIT.class.getSimpleName() + "-" + label;
      final Set<String> allTopics = admin.listTopics(KafkaDefaults.TOPIC_OPERATION);
      final List<String> testTopics = allTopics.stream().filter(topic -> topic.startsWith(topicPrefix)).collect(Collectors.toList());
      admin.deleteTopics(testTopics, KafkaDefaults.TOPIC_OPERATION);
      final String topicName = topicPrefix + "-" + new SimpleDateFormat(KafkaDefaults.TOPIC_DATE_FORMAT).format(new Date());
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
    
    final Map<Integer, AtomicIntegerArray> receiveCountsPerPartition = new TreeMap<>();
    for (int p = 0; p < partitions; p++) {
      receiveCountsPerPartition.put(p, new AtomicIntegerArray(messages));
    }
    final AtomicInteger totalReceived = new AtomicInteger();
    final AtomicInteger totalConfirmed = new AtomicInteger();
    executor = Executors.newFixedThreadPool(8);
    
    // The minimum an received offset must move by before adding a new consumer. This value shouldn't be smaller
    // than two, as it guarantees a clean boundary between consumer rebalancing.
    final int minOffsetMove = 10;
    
    new Thread(() -> {
      int[] latestOffsetsFromLastRebalance = getLatestOffsets(receiveCountsPerPartition);
      for (int c = 0; c < consumers; c++) {
        if (c != 0) {
          Threads.sleep(consumerJoinIntervalMillis);
          for (;;) {
            final int[] latestOffsets = getLatestOffsets(receiveCountsPerPartition);
            if (offsetsMoved(latestOffsetsFromLastRebalance, latestOffsets, minOffsetMove)) {
              latestOffsetsFromLastRebalance = latestOffsets;
              break;
            } else {
              zlg.d("Offsets haven't moved significantly since last rebalance: %s",
                    z -> z.arg(Arrays.toString(latestOffsets)));
              Threads.sleep(10);
            }
          }
        }

        zlg.i("Attaching consumer");
        ledger.attach(new MessageHandler() {
          @Override
          public String getGroupId() {
            return "group";
          }
    
          @Override
          public void onMessage(MessageContext context, Message message) {
            final Confirmation confirmation = context.begin(message);
            final int partition = ((DefaultMessageId) message.getMessageId()).getShard();
            final long offset = ((DefaultMessageId) message.getMessageId()).getOffset();
            if (offset == messages - 1) {
              zlg.d("Received last message at offset %d (partition %d)", z -> z.arg(offset).arg(partition));
            }
            
            final AtomicIntegerArray receiveCounts = receiveCountsPerPartition.get(partition);
            final int count = receiveCounts.incrementAndGet((int) offset);
            
            // log any message that has been processed more than once (unless it is a one-off)
            final int lastCount = offset == 0 ? 1 : receiveCounts.get((int) offset - 1);
            if (lastCount == 1 && count > 2 || lastCount == 2 && count > 1) {
              System.err.format("FAILURE in message %s; count=%d\n", message.getMessageId(), count);
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
    Wait.MEDIUM.untilTrue(() -> getUniqueCount(receiveCountsPerPartition) == expectedMessages);
    
    for (Map.Entry<Integer, AtomicIntegerArray> countEntry : receiveCountsPerPartition.entrySet()) {
      final int partition = countEntry.getKey();
      final AtomicIntegerArray receiveCounts = countEntry.getValue();
      // the counts must all be 1 or 2; we only allow a count of 2 for one-off messages, signifying
      // an overlap between consumer rebalancing (which is a Kafka thing)
      int lastReceiveCount = 1;
      boolean passed = false;
      try {
        for (int offset = 0; offset < receiveCounts.length(); offset++) {
          final int receiveCount = receiveCounts.get(offset);
          if (lastReceiveCount == 2) {
            assertTrue("offset=" + offset, receiveCount == 1);
          } else {
            assertTrue("offset=" + offset, receiveCount == 1 || receiveCount == 2);
          }
          lastReceiveCount = receiveCount;
        }
        passed = true;
      } finally {
        if (! passed) {
          System.err.println("partition: " + partition);
          for (int offset = 0; offset < receiveCounts.length(); offset++) {
            System.err.println("  offset: " + offset + ", count: " + receiveCounts.get(offset));
          }
        }
      }
    }

    ledger.dispose();
    
    zlg.i("Awaiting final confirmations");
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() >= getUniqueCount(receiveCountsPerPartition));
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() == totalConfirmed.get());
    zlg.i("Test passed: %s", z -> z.arg(testLabel));
  }
  
  private static int getUniqueCount(Map<Integer, AtomicIntegerArray> receiveCountsPerPartition) {
    int totalUnique = 0;
    for (AtomicIntegerArray receiveCounts : receiveCountsPerPartition.values()) {
      for (int offset = 0; offset < receiveCounts.length(); offset++) {
        if (receiveCounts.get(offset) != 0) {
          totalUnique++;
        }
      }
    }
    return totalUnique;
  }

  private static int[] getLatestOffsets(Map<Integer, AtomicIntegerArray> receiveCountsPerPartition) {
    final int[] latestOffsets = new int[receiveCountsPerPartition.size()];
    for (Map.Entry<Integer, AtomicIntegerArray> receiveCountsEntry : receiveCountsPerPartition.entrySet()) {
      latestOffsets[receiveCountsEntry.getKey()] = getLatestOffset(receiveCountsEntry.getValue());
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
