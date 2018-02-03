package com.obsidiandynamics.blackstrom.codec;

import com.obsidiandynamics.blackstrom.model.*;

public final class NullMessageCodec implements MessageCodec {
  @Override
  public byte[] encode(Message message) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message decode(byte[] bytes) throws Exception {
    throw new UnsupportedOperationException();
  }
}
