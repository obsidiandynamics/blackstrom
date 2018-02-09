package com.obsidiandynamics.blackstrom.ledger;

import org.junit.*;
import org.junit.runner.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class KafkaKryoLedgerIT extends AbstractLedgerTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaDocker.start();
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.MEDIUM;
  }
  
  @Override
  protected Ledger createLedger() {
    final Kafka<String, Message> kafka = 
        new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers("localhost:9092"));
    final String topicName = KafkaTopic.forTest(KafkaKryoLedgerIT.class, 
                                                "kryo-" + KryoMessageCodec.ENCODING_VERSION);
    return new KafkaLedger(new KafkaLedgerOptions()
                           .withKafka(kafka)
                           .withTopic(topicName)
                           .withCodec(new KryoMessageCodec(true, new KryoBankExpansion())));
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(KafkaKryoLedgerIT.class);
  }
}
