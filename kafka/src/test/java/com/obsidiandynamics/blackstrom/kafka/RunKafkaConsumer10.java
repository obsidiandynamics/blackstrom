package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

public final class RunKafkaConsumer10 {
  public static void main(String[] args) {
    final Properties props = new PropertiesBuilder()
        .with("bootstrap.servers", "localhost:9092")
        .with("group.id", "sample")
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", StringDeserializer.class.getName())
        .build();
    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props); 
    consumer.subscribe(Arrays.asList("test"));
    
    try {
      while (true) {
        System.out.println("polling...");
        final ConsumerRecords<String, String> records = consumer.poll(1_000);
        System.out.format("got %,d records\n", list(records).size());
        for (ConsumerRecord<String, String> record : records) {
          System.out.format("%,d: %s\n", record.offset(), record.value());
        }
      }
    } finally {
      consumer.close();
    }
  }
  
  private static List<ConsumerRecord<String, String>> list(ConsumerRecords<String, String> records) {
    final List<ConsumerRecord<String, String>> list = new ArrayList<>();
    records.iterator().forEachRemaining(list::add);
    return list;
  }
}
