package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

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
    TestKafka.start();
  }

  private final KafkaClusterConfig config = new KafkaClusterConfig().withBootstrapServers(TestKafka.bootstrapServers());

  private static String getTopic(Guidance guidance) {
    return TestTopic.of(KafkaBankTransferFailureIT.class, "json", JacksonMessageCodec.ENCODING_VERSION, guidance);
  }

  @Before
  public void before() throws Exception {
    try (var admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      admin.describeCluster(KafkaDefaults.CLUSTER_AWAIT);
      TestTopic.createTopics(admin, TestTopic.newOf(getTopic(Guidance.AUTONOMOUS)), TestTopic.newOf(getTopic(Guidance.COORDINATED)));
    }
  }

  @Override
  protected Ledger createLedger(Guidance guidance) {
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withKafka(new KafkaCluster<>(config))
                           .withDrainConfirmationsTimeout(5_000)
                           .withTopic(getTopic(guidance))
                           .withConsumerPipeConfig(new ConsumerPipeConfig().withAsync(false))
                           .withProducerPipeConfig(new ProducerPipeConfig().withAsync(false))
                           .withCodec(new JacksonMessageCodec(true, new JacksonBankExpansion())));
  }

  @Override
  protected Timesert getWait() {
    return Wait.LONG;
  }
}
