package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaMessageSerializer implements Serializer<Message> {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
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
      zlg.e("Error serializing message %s", z -> z.arg(data).threw(e));
      throw new MessageSerializationException("Error serializing message", e);
    }
  }

  @Override
  public void close() {}
}
