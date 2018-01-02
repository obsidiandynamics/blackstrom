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
  
  @YInject
  private Properties common = new Properties();

  @YInject
  private Properties producer = new Properties();
  
  @YInject
  private Properties consumer = new Properties();
  
  void init() {
    if (common.getProperty("bootstrap.servers") == null) {
      throw new IllegalArgumentException("Must specify a value for 'bootstrap.servers'");
    }
  }
  
  Properties getCommonProps() {
    return common;
  }
  
  KafkaClusterConfig withCommonProps(Properties common) {
    this.common = common;
    return this;
  }

  Properties getProducerProps() {
    final Properties props = getCommonProps();
    props.putAll(producer);
    return props;
  }
  
  KafkaClusterConfig withProducerProps(Properties producer) {
    this.producer = producer;
    return this;
  }
  
  Properties getConsumerProps() {
    final Properties props = getCommonProps();
    props.putAll(consumer);
    return props;
  }
  
  KafkaClusterConfig withConsumerProps(Properties consumer) {
    this.consumer = consumer;
    return this;
  }

  @Override
  public String toString() {
    return KafkaClusterConfig.class.getSimpleName() + " [common: " + common + ", producer: " + producer + ", consumer: " + consumer + "]";
  }
}
