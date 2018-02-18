package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.slf4j.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class KafkaGroupLedgerIT extends AbstractGroupLedgerTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(KafkaGroupLedgerIT.class);
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaDocker.start();
  }
  
  private final String topic = getTopic();
  
  @Before
  public void before() throws InterruptedException, ExecutionException {
    final AdminClient admin = AdminClient.create(new PropertiesBuilder()
                                                 .with("bootstrap.servers", "localhost:9092")
                                                 .build());
    final NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
    final CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
    for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
      try {
        entry.getValue().get();
        LOG.debug("Created topic {}", entry.getKey());
      } catch (ExecutionException e) {
        if (e.getCause() instanceof TopicExistsException) {
          LOG.debug("Topic {} already exists", entry.getKey());
        } else {
          throw e;
        }
      }
    }
  }
  
  private static String getTopic() {
    return TestTopic.of(KafkaGroupLedgerIT.class, "json-" + System.currentTimeMillis(), JacksonMessageCodec.ENCODING_VERSION);
  }
  
  @Override
  protected Timesert getWait() {
    return Wait.MEDIUM;
  }
  
  @Override
  protected Ledger createLedger() {
    final Kafka<String, Message> kafka = 
        new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers("localhost:9092"));
    return new KafkaLedger(new KafkaLedgerConfig()
                           .withKafka(kafka)
                           .withTopic(topic)
                           .withCodec(new JacksonMessageCodec(false)));
  }
}
