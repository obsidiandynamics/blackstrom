package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class KafkaJacksonGroupLedgerIT extends AbstractGroupLedgerTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaDocker.start();
  }
  
  private final KafkaClusterConfig config = new KafkaClusterConfig().withBootstrapServers("localhost:9092");
  
  private final String topic = TestTopic.of(KafkaJacksonGroupLedgerIT.class, "json", JacksonMessageCodec.ENCODING_VERSION);
  
  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaTimeouts.CLUSTER_AWAIT);
      admin.ensureExists(TestTopic.newOf(topic), KafkaTimeouts.TOPIC_CREATE);
    }
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.MEDIUM;
  }
  
  @Override
  protected Ledger createLedger() {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withKafka(new KafkaCluster<>(config))
                           .withTopic(topic)
                           .withCodec(new JacksonMessageCodec(false)));
  }
}
