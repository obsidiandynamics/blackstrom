package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class KafkaKryoRandomBankTransferIT extends AbstractRandomBankTransferTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaDocker.start();
  }
  
  @Override
  protected Ledger createLedger() {
    final String topicBaseName = KafkaTopic.forTest(KafkaKryoRandomBankTransferIT.class, 
                                                    "kryo-" + KryoMessageCodec.ENCODING_VERSION);
    final Kafka<String, Message> kafka = 
        new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers("localhost:9092"));
    return new KafkaLedger(new KafkaLedgerOptions()
                           .withKafka(kafka)
                           .withTopic(topicBaseName + (Testmark.isEnabled() ? ".bench" : ""))
                           .withCodec(new KryoMessageCodec(true, new KryoBankExpansion())));
  }

  @Override
  protected Timesert getWait() {
    return Wait.MEDIUM;
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(KafkaKryoRandomBankTransferIT.class);
  }
}
