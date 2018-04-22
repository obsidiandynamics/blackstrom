package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class KafkaMessageSerializer implements Serializer<Message> {
  private static final Logger log = LoggerFactory.getLogger(KafkaMessageSerializer.class);
  
  static final class MessageSerializationException extends KafkaException {
    private static final long serialVersionUID = 1L;

    MessageSerializationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
  
  private MessageCodec codec;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    codec = CodecRegistry.forLocator((String) configs.get(CodecRegistry.CONFIG_CODEC_LOCATOR));
  }

  @Override
  public byte[] serialize(String topic, Message data) {
    try {
      return codec.encode(data);
    } catch (Throwable e) {
      log.error("Error serializing message " + data, e);
      throw new MessageSerializationException("Error serializing message", e);
    }
  }

  @Override
  public void close() {}
}
