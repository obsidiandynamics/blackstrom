package com.obsidiandynamics.blackstrom.ledger;

import java.util.concurrent.*;

import org.junit.*;
import org.junit.runner.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class KafkaJacksonLedgerIT extends AbstractLedgerTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaDocker.start();
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.MEDIUM;
  }
  
  private final String topic = TestTopic.of(KafkaJacksonLedgerIT.class, "json", JacksonMessageCodec.ENCODING_VERSION);
  
  private final KafkaClusterConfig config = new KafkaClusterConfig().withBootstrapServers("localhost:9092");
  
  @Before
  public void before() throws InterruptedException, ExecutionException {
    KafkaAdmin.forConfig(config).ensureExists(topic);
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
