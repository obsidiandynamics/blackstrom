package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.format.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaMessageDeserializer implements Deserializer<Message> {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  static final class MessageDeserializationException extends KafkaException {
    private static final long serialVersionUID = 1L;

    MessageDeserializationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
  
  private MessageCodec codec;
  
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    codec = CodecRegistry.forLocator((String) configs.get(CodecRegistry.CONFIG_CODEC_LOCATOR));
    mustExist(codec);
  }

  @Override
  public Message deserialize(String topic, byte[] data) {
    try {
      return codec.decode(data);
    } catch (Throwable e) {
      zlg.e("Error deserializing message\n%s", 
            z -> z.arg(Args.map(Args.ref(data), Binary::dump)).threw(e));
      throw new MessageDeserializationException("Error deserializing message", e);
    }
  }

  @Override
  public void close() {}
}
