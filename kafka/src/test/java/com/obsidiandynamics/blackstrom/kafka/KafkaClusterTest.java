package com.obsidiandynamics.blackstrom.kafka;

import static org.junit.Assert.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.*;

public class KafkaClusterTest {
  private static <K, V> KafkaCluster<K, V> createCluster() {
    final KafkaClusterConfig config = new KafkaClusterConfig()
        .withCommonProps(new PropertiesBuilder()
                         .with("bootstrap.servers", "localhost:9092")
                         .build())
        .withProducerProps(new PropertiesBuilder()
                           .with("key.serializer", StringSerializer.class.getName())
                           .with("value.serializer", StringSerializer.class.getName())
                           .build())
        .withConsumerProps(new PropertiesBuilder()
                           .with("key.deserializer", StringDeserializer.class.getName())
                           .with("value.deserializer", StringDeserializer.class.getName())
                           .build());
    return new KafkaCluster<>(config);
  }
  
  @Test
  public void testProducer() {
    try (Producer<?, ?> producer = createCluster().getProducer(new Properties())) {
      assertNotNull(producer);
    }
  }
  
  @Test
  public void testConsumer() {
    try (Consumer<?, ?> producer = createCluster().getConsumer(new Properties())) {
      assertNotNull(producer);
    }
  }
}
