package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaJacksonMessageSerializer implements Serializer<Message> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public byte[] serialize(String topic, Message data) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

}
