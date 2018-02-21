package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import com.obsidiandynamics.yconf.*;

@Y(KafkaClusterConfig.Mapper.class)
public final class KafkaClusterConfig {
  public static final class Mapper implements TypeMapper {
    @Override public Object map(YObject y, Class<?> type) {
      return new KafkaClusterConfig()
          .withCommonProps(y.mapAttribute("common", PropertiesBuilder.class).build())
          .withProducerProps(y.mapAttribute("producer", PropertiesBuilder.class).build())
          .withConsumerProps(y.mapAttribute("consumer", PropertiesBuilder.class).build());
    }
  }
  
  public static final String CONFIG_BOOTSTRAP_SERVERS = "bootstrap.servers";
  
  private Properties common = new PropertiesBuilder()
      .withSystemDefault(CONFIG_BOOTSTRAP_SERVERS, null)
      .build();

  private Properties producer = new Properties();
  
  private Properties consumer = new Properties();
  
  public void validate() {
    if (common.getProperty(CONFIG_BOOTSTRAP_SERVERS) == null) {
      throw new IllegalArgumentException("Must specify a value for '" + CONFIG_BOOTSTRAP_SERVERS + "'");
    }
  }
  
  public KafkaClusterConfig withBootstrapServers(String bootstrapServers) {
    return withCommonProps(Collections.singletonMap(CONFIG_BOOTSTRAP_SERVERS, bootstrapServers));
  }
  
  public Properties getCommonProps() {
    return common;
  }
  
  private Properties getCommonPropsCopy() {
    final Properties props = new Properties();
    props.putAll(common);
    return props;
  }
  
  public KafkaClusterConfig withCommonProps(Map<Object, Object> common) {
    this.common.putAll(common);
    return this;
  }

  public Properties getProducerCombinedProps() {
    final Properties props = getCommonPropsCopy();
    props.putAll(producer);
    return props;
  }
  
  public KafkaClusterConfig withProducerProps(Map<Object, Object> producer) {
    this.producer.putAll(producer);
    return this;
  }
  
  public Properties getConsumerCombinedProps() {
    final Properties props = getCommonPropsCopy();
    props.putAll(consumer);
    return props;
  }
  
  public KafkaClusterConfig withConsumerProps(Map<Object, Object> consumer) {
    this.consumer.putAll(consumer);
    return this;
  }

  @Override
  public String toString() {
    return KafkaClusterConfig.class.getSimpleName() + " [common: " + common + ", producer: " + 
        producer + ", consumer: " + consumer + "]";
  }
}
