package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.admin.*;
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
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    new KafkaDocker().withComposeFile("stack/docker-compose.yaml").start();
  }
  
  private final KafkaClusterConfig config = new KafkaClusterConfig()
      .withBootstrapServers("localhost:9092")
      .withConsumerProps(new MapBuilder<Object, Object>()
                         .build());
  
  private final String topic = TestTopic.of(KafkaLedgerDrainConfirmationsIT.class, "kryo", KryoMessageCodec.ENCODING_VERSION);
  
  private final Sandbox sandbox = Sandbox.forInstance(this);
  
  private ExecutorService executor;
  
  private KafkaLedger ledger;
  
  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaTimeouts.CLUSTER_AWAIT);
      admin.createTopics(Collections.singleton(TestTopic.newOf(topic)), KafkaTimeouts.TOPIC_CREATE);
    }
  }
  
  @After
  public void after() {
    if (executor != null) {
      executor.shutdown();
    }
    
    if (ledger != null) {
      ledger.dispose();
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
    final Zlg zlg = Zlg.forDeclaringClass().get();
    zlg.i("Starting test");
    
    final int messages = 400;
    final int messageIntervalMillis = 10;
    final int consumers = 4;
    final int consumerJoinIntervalMillis = 1000;
    final int commitDelayMillis = 100;
    
    ledger = new KafkaLedger(new KafkaLedgerConfig()
                             .withKafka(new KafkaCluster<>(config))
                             .withTopic(topic)
                             .withDrainConfirmations(true)
                             .withCodec(new KryoMessageCodec(true)));
    ledger.init();
    
    final Map<Long, AtomicInteger> receiveCounts = new ConcurrentHashMap<>();
    final AtomicInteger totalReceived = new AtomicInteger();
    executor = Executors.newFixedThreadPool(8);
    
    new Thread(() -> {
      for (int c = 0; c < consumers; c++) {
        if (c != 0) Threads.sleep(consumerJoinIntervalMillis);

        zlg.i("Attaching consumer");
        ledger.attach(new MessageHandler() {
          int lastCount = 1;
          
          @Override
          public String getGroupId() {
            return "group";
          }
    
          @Override
          public void onMessage(MessageContext context, Message message) {
            if (sandbox.contains(message)) {
              final Confirmation confirmation = context.begin(message);
              final long offset = ((DefaultMessageId) message.getMessageId()).getOffset();
              final AtomicInteger count = receiveCounts.compute(offset, (__, _count) -> {
                if (_count == null) {
                  return new AtomicInteger(1);
                } else {
                  _count.incrementAndGet();
                  return _count;
                }
              });
              
              // log any message that has been processed more than once (unless it is a one-off)
              final int currentCount = count.get();
              if (lastCount == 1 && currentCount > 2 || lastCount == 2 && currentCount > 1) {
                System.err.format("FAILURE in message %s; count=%d\n", message, currentCount);
              }
              lastCount = count.get();
              totalReceived.incrementAndGet();
              
              // delay offset commits for a more pronounced effect (increases likelihood of repeated messages
              // after rebalancing)
              final long confirmNoSoonerThan = System.currentTimeMillis() + commitDelayMillis;
              try {
                executor.submit(() -> {
                  final long sleepNeeded = confirmNoSoonerThan - System.currentTimeMillis();
                  Threads.sleep(sleepNeeded);
                  confirmation.confirm();
                });
              } catch (RejectedExecutionException e) {
                zlg.i("Executor terminated: confirming offset %d in handler thread", z -> z.arg(offset));
                confirmation.confirm();
              }
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
      ledger.append(new Command(String.valueOf(m), null, 0)
                    .withMessageId(new DefaultMessageId(0, m))
                    .withShardKey(sandbox.key()));
      Threads.sleep(messageIntervalMillis);

      // wait for at least one message to be received before publishing more
      // (prevents over-publishing before at a consumer has been assigned a partition)
      Wait.MEDIUM.untilTrue(() -> totalReceived.get() >= 1);
    }
    
    // await receival of all messages
    Wait.MEDIUM.untilTrue(() -> receiveCounts.size() == messages);
    
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
        for (Map.Entry<Long, AtomicInteger> receiveCountEntry : receiveCounts.entrySet()) {
          System.err.println("entry: " + receiveCountEntry);
        }
      }
    }
    
    zlg.i("Test passed");
  }
}
