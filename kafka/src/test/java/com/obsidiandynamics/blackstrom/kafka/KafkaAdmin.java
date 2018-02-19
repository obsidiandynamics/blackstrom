package com.obsidiandynamics.blackstrom.kafka;

import static com.obsidiandynamics.blackstrom.kafka.KafkaClusterConfig.*;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.slf4j.*;

public final class KafkaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(TestTopic.class);
  
  private final AdminClient admin;
  
  private KafkaAdmin(AdminClient admin) {
    this.admin = admin;
  }
  
  public static KafkaAdmin forConfig(KafkaClusterConfig config) {
    final String bootstrapServers = config.getCommonProps().getProperty(CONFIG_BOOTSTRAP_SERVERS);
    final AdminClient admin = AdminClient.create(new PropertiesBuilder()
                                                 .with(CONFIG_BOOTSTRAP_SERVERS, bootstrapServers)
                                                 .build());
    return new KafkaAdmin(admin);
  }
  
  public void ensureExists(String topic) throws InterruptedException, ExecutionException {
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
}
