package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.admin.*;
import org.junit.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.threads.*;

public final class KafkaLedgerExactlyOnceIT {
  @BeforeClass
  public static void beforeClass() throws Exception {
    new KafkaDocker().withComposeFile("stack/docker-compose.yaml").start();
  }
  
  private final KafkaClusterConfig config = new KafkaClusterConfig().withBootstrapServers("localhost:9092");
  
  private final String topic = TestTopic.of(KafkaLedgerExactlyOnceIT.class, "kryo", KryoMessageCodec.ENCODING_VERSION);
  
  private final Sandbox sandbox = Sandbox.forInstance(this);
  
  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaTimeouts.CLUSTER_AWAIT);
      admin.createTopics(Collections.singleton(TestTopic.newOf(topic)), KafkaTimeouts.TOPIC_CREATE);
    }
  }
  
  @Test
  public void testMultipleConsumers() {
    final int messages = 100;
    final int messageIntervalMillis = 1;
    final int consumers = 1;
    
    final KafkaLedger ledger = new KafkaLedger(new KafkaLedgerConfig()
                                               .withKafka(new KafkaCluster<>(config))
                                               .withTopic(topic)
                                               .withCodec(new KryoMessageCodec(true)));
    ledger.init();
    
    final Map<String, AtomicInteger> receiveCounts = new ConcurrentHashMap<>();
    final AtomicInteger totalReceived = new AtomicInteger();
    for (int c = 0; c < consumers; c++) {
      ledger.attach(new MessageHandler() {
        @Override
        public String getGroupId() {
          return "group";
        }
  
        @Override
        public void onMessage(MessageContext context, Message message) {
          if (sandbox.contains(message)) {
            final String id = message.getXid();
            final AtomicInteger counter = receiveCounts.computeIfAbsent(id, __ -> new AtomicInteger());
            final int count = counter.incrementAndGet();
            if (count != 1) {
              System.err.format("FAILURE in message %s; count=%d\n", message, count);
            }
            totalReceived.incrementAndGet();
            context.beginAndConfirm(message);
          }
        }
      });
    }
    
    for (int m = 0; m < messages; m++) {
      ledger.append(new Command(String.valueOf(m), null, 0)
                    .withMessageId(new DefaultMessageId(0, m))
                    .withShardKey(sandbox.key()));
      Threads.sleep(messageIntervalMillis);
    }
    
    // await receival of all messages
    Wait.MEDIUM.untilTrue(() -> totalReceived.get() == messages);
    
    // the counts must all be 1
    for (Map.Entry<String, AtomicInteger> receiveCountEntry : receiveCounts.entrySet()) {
      assertEquals("xid=" + receiveCountEntry.getKey(), 1, receiveCountEntry.getValue().get());
    }
  }
}
