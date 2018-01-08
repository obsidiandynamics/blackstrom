package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaJacksonMessageSerializer implements Serializer<Message> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaJacksonMessageSerializer.class);
  
  static final class MessageSerializationException extends KafkaException {
    private static final long serialVersionUID = 1L;

    MessageSerializationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
  
  private MessageCodec codec;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    codec = new JacksonMessageCodec(false);
  }

  @Override
  public byte[] serialize(String topic, Message data) {
    final String encoded;
    try {
      encoded = codec.encode(data);
    } catch (Throwable e) {
      LOG.error("Error serializing message " + data, e);
      throw new MessageSerializationException("Error serializing message", e);
    }
    return encoded.getBytes();
  }

  @Override
  public void close() {}
}
