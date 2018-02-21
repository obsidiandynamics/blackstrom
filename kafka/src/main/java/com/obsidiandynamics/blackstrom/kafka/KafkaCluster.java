package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.yconf.*;

@Y
public final class KafkaCluster<K, V> implements Kafka<K, V> {
  private final KafkaClusterConfig config;
  
  public KafkaCluster(@YInject(name="clusterConfig") KafkaClusterConfig config) {
    config.validate();
    this.config = config;
  }
  
  public KafkaClusterConfig getConfig() {
    return config;
  }

  @Override
  public Producer<K, V> getProducer(Properties defaults, Properties overrides) {
    final Properties combinedProps = new Properties();
    combinedProps.putAll(defaults);
    combinedProps.putAll(config.getProducerCombinedProps());
    combinedProps.putAll(overrides);
    return new KafkaProducer<>(combinedProps);
  }

  @Override
  public Consumer<K, V> getConsumer(Properties defaults, Properties overrides) {
    final Properties combinedProps = new Properties();
    combinedProps.putAll(defaults);
    combinedProps.putAll(config.getConsumerCombinedProps());
    combinedProps.putAll(overrides);
    return new KafkaConsumer<>(combinedProps);
  }

  @Override
  public String toString() {
    return KafkaCluster.class.getSimpleName() + " [config: " + config + "]";
  }
}
