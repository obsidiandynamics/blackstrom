package com.obsidiandynamics.blackstrom.bank;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class KafkaBankTransferFailureIT extends AbstractBankTransferFailureTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    new KafkaDocker().withComposeFile("stack/docker-compose.yaml").start();
  }
  
  private final KafkaClusterConfig config = new KafkaClusterConfig().withBootstrapServers("localhost:9092");
  
  private static String getTopic(Guidance guidance) {
    return TestTopic.of(KafkaBankTransferFailureIT.class, "json", JacksonMessageCodec.ENCODING_VERSION, guidance);
  }
  
  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaTimeouts.CLUSTER_AWAIT);
      admin.createTopics(Collections.singleton(TestTopic.newOf(getTopic(Guidance.AUTONOMOUS))), KafkaTimeouts.TOPIC_OPERATION);
      admin.createTopics(Collections.singleton(TestTopic.newOf(getTopic(Guidance.COORDINATED))), KafkaTimeouts.TOPIC_OPERATION);
    }
  }
  
  @Override
  protected Ledger createLedger(Guidance guidance) {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withKafka(new KafkaCluster<>(config))
                           .withTopic(getTopic(guidance))
                           .withCodec(new JacksonMessageCodec(true, new JacksonBankExpansion())));
  }

  @Override
  protected Timesert getWait() {
    return Wait.LONG;
  }
}
