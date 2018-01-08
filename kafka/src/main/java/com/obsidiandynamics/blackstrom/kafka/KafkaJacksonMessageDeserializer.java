package com.obsidiandynamics.blackstrom.kafka;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaJacksonMessageDeserializer implements Deserializer<Message> {
  public static final String CONFIG_MAP_PAYLOAD = "value.deserializer.mapPayload";
  
  private static final Logger LOG = LoggerFactory.getLogger(KafkaJacksonMessageDeserializer.class);
  
  static final class MessageDeserializationException extends KafkaException {
    private static final long serialVersionUID = 1L;

    MessageDeserializationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
  
  private MessageCodec codec;
  
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    final Object mapPayloadConfigValue = configs.get(CONFIG_MAP_PAYLOAD);
    final boolean mapPayload;
    try {
      mapPayload = Boolean.parseBoolean(String.valueOf(mapPayloadConfigValue));
    } catch (Throwable e) {
      LOG.error("Error configuring deserializer", e);
      throw new ConfigException(CONFIG_MAP_PAYLOAD, mapPayloadConfigValue);
    }
    
    codec = new JacksonMessageCodec(mapPayload);
  }

  @Override
  public Message deserialize(String topic, byte[] data) {
    try {
      final String encoded = new String(data);
      return codec.decode(encoded);
    } catch (Throwable e) {
      LOG.error("Error deserializing message " + data, e);
      throw new MessageDeserializationException("Error deserializing message", e);
    }
  }

  @Override
  public void close() {}
}
