package com.obsidiandynamics.blackstrom.codec;

import com.obsidiandynamics.blackstrom.model.*;

public interface MessageCodec {
  static int MODEL_VERSION = 2;
  
  byte[] encode(Message message) throws Exception;
  
  default String encodeText(Message message) throws Exception {
    return new String(encode(message));
  }
  
  Message decode(byte[] bytes) throws Exception;
  
  default Message decodeText(String text) throws Exception {
    return decode(text.getBytes());
  }
}
