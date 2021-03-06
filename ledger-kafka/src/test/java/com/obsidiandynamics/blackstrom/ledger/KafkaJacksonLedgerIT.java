package com.obsidiandynamics.blackstrom.ledger;

import org.apache.kafka.clients.admin.*;
import org.junit.*;
import org.junit.runner.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.testmark.*;

public final class KafkaJacksonLedgerIT extends AbstractLedgerTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    TestKafka.start();
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.LONG;
  }
  
  private final String topic = TestTopic.of(KafkaJacksonLedgerIT.class, "json", JacksonMessageCodec.ENCODING_VERSION);
  
  private final KafkaClusterConfig config = new KafkaClusterConfig().withBootstrapServers(TestKafka.bootstrapServers());
  
  @Before
  public void before() throws Exception {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaDefaults.CLUSTER_AWAIT);
      TestTopic.createTopics(admin, TestTopic.newOf(topic));
    }
  }
  
  @Override
  protected Ledger createLedger() {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withKafka(new KafkaCluster<>(config))
                           .withTopic(topic)
                           .withCodec(new JacksonMessageCodec(true, new JacksonBankExpansion())));
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(KafkaJacksonLedgerIT.class);
  }
}
