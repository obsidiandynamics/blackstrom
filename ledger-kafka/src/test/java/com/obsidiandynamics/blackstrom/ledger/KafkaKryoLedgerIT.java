package com.obsidiandynamics.blackstrom.ledger;

import org.apache.kafka.clients.admin.*;
import org.junit.*;
import org.junit.runner.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.testmark.*;

public final class KafkaKryoLedgerIT extends AbstractLedgerTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    new KafkaDocker().withComposeFile("stack/docker-compose.yaml").start();
  }
  
  private final KafkaClusterConfig config = new KafkaClusterConfig().withBootstrapServers("localhost:9092");
  
  private final String topic = TestTopic.of(KafkaKryoLedgerIT.class, "kryo", KryoMessageCodec.ENCODING_VERSION);
  
  @Before
  public void before() throws Exception {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaDefaults.CLUSTER_AWAIT);
      TestTopic.createTopics(admin, TestTopic.newOf(topic));
    }
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.LONG;
  }
  
  @Override
  protected Ledger createLedger() {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withKafka(new KafkaCluster<>(config))
                           .withTopic(topic)
                           .withCodec(new KryoMessageCodec(true, new KryoBankExpansion())));
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(KafkaKryoLedgerIT.class);
  }
}
