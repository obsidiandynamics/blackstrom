package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

public interface Kafka<K, V> {
  default Producer<K, V> getProducer(Properties overrides) {
    return getProducer(new Properties(), overrides);
  }
  
  Producer<K, V> getProducer(Properties defaults, Properties overrides);
  
  void describeProducer(java.util.function.Consumer<String> logLine, Properties defaults, Properties overrides);
  
  default Consumer<K, V> getConsumer(Properties overrides) {
    return getConsumer(new Properties(), overrides);
  }
  
  Consumer<K, V> getConsumer(Properties defaults, Properties overrides);
  
  void describeConsumer(java.util.function.Consumer<String> logLine, Properties defaults, Properties overrides);
}
