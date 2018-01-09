package com.obsidiandynamics.blackstrom.kafka;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.yconf.*;

public final class KafkaConfigTest {
  @Test
  public void testConfig() throws IOException {
    final Kafka<?, ?> kafka = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(KafkaConfigTest.class.getClassLoader().getResourceAsStream("kafka-config.yaml"))
        .map(Kafka.class);
    assertNotNull(kafka);
    assertEquals(KafkaCluster.class, kafka.getClass());
    Assertions.assertToStringOverride(kafka);
    
    final KafkaClusterConfig config = ((KafkaCluster<?, ?>) kafka).getConfig();
    assertNotNull(config);
    assertEquals(new PropertiesBuilder().with(KafkaClusterConfig.CONFIG_BOOTSTRAP_SERVERS, "10.20.30.40:9092").build(),
                 config.getCommonProps());
    Assertions.assertToStringOverride(config);
  }
  
  @Test
  public void testApi() {
    final Properties commonProps = new PropertiesBuilder().with("common", "COMMON").build();
    final Properties producerProps = new PropertiesBuilder().with("producer", "PRODUCER").build();
    final Properties consumerProps = new PropertiesBuilder().with("consumer", "CONSUMER").build();
    
    final KafkaClusterConfig config = new KafkaClusterConfig()
        .withCommonProps(commonProps)
        .withProducerProps(producerProps)
        .withConsumerProps(consumerProps);
    
    assertEquals(commonProps, config.getCommonProps());

    producerProps.putAll(commonProps);
    assertEquals(producerProps, config.getProducerCombinedProps());

    consumerProps.putAll(commonProps);
    assertEquals(consumerProps, config.getConsumerCombinedProps());
  }  
  
  @Test
  public void testBootstrapServers() {
    final KafkaClusterConfig config = new KafkaClusterConfig()
        .withBootstrapServers("localhost:9092");
    
    assertEquals("localhost:9092", config.getCommonProps().getProperty(KafkaClusterConfig.CONFIG_BOOTSTRAP_SERVERS));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testNoBoostrapServers() {
    new KafkaClusterConfig().init();
  }
}
