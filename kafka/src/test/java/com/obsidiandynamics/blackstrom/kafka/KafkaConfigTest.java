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
    assertEquals(new PropertiesBuilder().with("bootstrap.servers", "10.20.30.40:9092").build(),
                 config.getCommonProps());
    Assertions.assertToStringOverride(config);
  }
  
  @Test
  public void testApi() {
    final Properties commonProps = new Properties();
    final Properties producerProps = new Properties();
    final Properties consumerProps = new Properties();
    
    final KafkaClusterConfig config = new KafkaClusterConfig()
        .withCommonProps(commonProps)
        .withProducerProps(producerProps)
        .withConsumerProps(consumerProps);
    
    assertEquals(commonProps, config.getCommonProps());
    assertEquals(producerProps, config.getProducerProps());
    assertEquals(consumerProps, config.getConsumerProps());
  }  
  
  @Test(expected=IllegalArgumentException.class)
  public void testNoBoostrapServers() {
    new KafkaClusterConfig().init();
  }
}
