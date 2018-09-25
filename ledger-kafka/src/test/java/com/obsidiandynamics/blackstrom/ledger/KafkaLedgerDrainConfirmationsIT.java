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
  
  /**
   *  Verifies that messages can be processed exactly once in the presence of multiple consumers
   *  contending for a single group ID. Message flow should transition from one consumer to the
   *  next as consumers are added; the prior consumer relinquishing partition assignments will
   *  drain any pending confirmations before yielding to the new assignee. Messages may still
   *  be processed twice at the handover boundary; this is just how Kafka works — the next
   *  assignee will resume processing from the last confirmed offset, even if it means processing
   *  the message twice. (The test accounts for this, by allowing one-off messages to have two
   *  receivals, providing that these don't occur in succession.)
   */
  @Test
  public void testMultipleConsumers() {
    zlg.i("Starting test");
    
    final int partitions = 16;
    final int messages = 400;
    final int messageIntervalMillis = 10;
    final int consumers = 4;
    final int consumerJoinIntervalMillis = 1000;
    final int commitDelayMillis = 100;
    
    final String topicName = Exceptions.wrap(() -> createTopic("MultiPartition", partitions), RuntimeException::new);
    
    ledger = new KafkaLedger(new KafkaLedgerConfig()
                             .withKafka(new KafkaCluster<>(config))
                             .withTopic(topicName)
                             .withDrainConfirmations(true)
                             .withDrainConfirmationsTimeout(Integer.MAX_VALUE)
                             .withZlg(zlg)
                             .withCodec(new KryoMessageCodec(true)));
    ledger.init();
    
    final Map<Integer, Map<Long, AtomicInteger>> receiveCountsPerPartition = new TreeMap<>();
    for (int p = 0; p < partitions; p++) {
      receiveCountsPerPartition.put(p, new ConcurrentHashMap<>());
    }
    final AtomicInteger totalReceived = new AtomicInteger();
    final AtomicInteger totalConfirmed = new AtomicInteger();
    executor = Executors.newFixedThreadPool(8);
    
    new Thread(() -> {
      for (int c = 0; c < consumers; c++) {
        if (c != 0) Threads.sleep(consumerJoinIntervalMillis);

        zlg.i("Attaching consumer");
        ledger.attach(new MessageHandler() {
          final int[] lastCounts = new int[partitions];
          {
            for (int p = 0; p < partitions; p++) {
              lastCounts[p] = 1;
            }
          }
          
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
            
            final Map<Long, AtomicInteger> receiveCounts = receiveCountsPerPartition.get(partition);
            final AtomicInteger count = receiveCounts.compute(offset, (__, _count) -> {
              if (_count == null) {
                return new AtomicInteger(1);
              } else {
                _count.incrementAndGet();
                return _count;
              }
            });
            
            // log any message that has been processed more than once (unless it is a one-off)
            final int lastCount = lastCounts[partition];
            final int currentCount = count.get();
            if (lastCount == 1 && currentCount > 2 || lastCount == 2 && currentCount > 1) {
              System.err.format("FAILURE in message %s; count=%d\n", message.getMessageId(), currentCount);
            }
            lastCounts[partition] = count.get();
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
    
    for (Map.Entry<Integer, Map<Long, AtomicInteger>> countEntry : receiveCountsPerPartition.entrySet()) {
      final int partition = countEntry.getKey();
      final Map<Long, AtomicInteger> receiveCounts = countEntry.getValue();
      // the counts must all be 1 or 2; we only allow a count of 2 for one-off messages, signifying
      // an overlap between consumer rebalancing (which is a Kafka thing)
      final SortedMap<Long, AtomicInteger> receiveCountsSorted = new TreeMap<>();
      receiveCountsSorted.putAll(receiveCounts);
      int lastReceiveCount = 1;
      boolean passed = false;
      try {
        for (Map.Entry<Long, AtomicInteger> receiveCountEntry : receiveCounts.entrySet()) {
          final int receiveCount = receiveCountEntry.getValue().get();
          if (lastReceiveCount == 2) {
            assertTrue("offset=" + receiveCountEntry.getKey(), receiveCount == 1);
          } else {
            assertTrue("offset=" + receiveCountEntry.getKey(), receiveCount == 1 || receiveCount == 2);
          }
          lastReceiveCount = receiveCount;
        }
        passed = true;
      } finally {
        if (! passed) {
          System.err.println("partition: " + partition);
          for (Map.Entry<Long, AtomicInteger> receiveCountEntry : receiveCounts.entrySet()) {
            System.err.println("  entry: " + receiveCountEntry);
          }
        }
      }
    }

    ledger.dispose();
    
    zlg.i("Awaiting final confirmations");
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() >= getUniqueCount(receiveCountsPerPartition));
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() == totalConfirmed.get());
    zlg.i("Test passed");
  }
  
  private static int getUniqueCount(Map<Integer, Map<Long, AtomicInteger>> receiveCountsPerPartition) {
    return receiveCountsPerPartition.values().stream().collect(Collectors.summingInt(Map::size));
  }
}
