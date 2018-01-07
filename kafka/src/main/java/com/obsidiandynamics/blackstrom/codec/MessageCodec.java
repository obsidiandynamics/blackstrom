package com.obsidiandynamics.blackstrom.codec;

import com.obsidiandynamics.blackstrom.model.*;

public interface MessageCodec {
  String encode(Message message) throws Exception;
  
  Message decode(String text) throws Exception;
}
