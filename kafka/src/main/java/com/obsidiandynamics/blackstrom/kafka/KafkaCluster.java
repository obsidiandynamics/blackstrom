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
  
  private static Properties mergeProps(Properties... propertiesArray) {
    final Properties merged = new Properties();
    Arrays.stream(propertiesArray).forEach(merged::putAll);
    return merged;
  }
  
  private Properties mergeProducerProps(Properties defaults, Properties overrides) {
    return mergeProps(defaults, config.getProducerCombinedProps(), overrides);
  }

  @Override
  public Producer<K, V> getProducer(Properties defaults, Properties overrides) {
    return new KafkaProducer<>(mergeProducerProps(defaults, overrides));
  }
  
  private Properties mergeConsumerProps(Properties defaults, Properties overrides) {
    return mergeProps(defaults, config.getConsumerCombinedProps(), overrides);
  }

  @Override
  public Consumer<K, V> getConsumer(Properties defaults, Properties overrides) {
    return new KafkaConsumer<>(mergeConsumerProps(defaults, overrides));
  }

  @Override
  public String toString() {
    return KafkaCluster.class.getSimpleName() + " [config: " + config + "]";
  }
}
