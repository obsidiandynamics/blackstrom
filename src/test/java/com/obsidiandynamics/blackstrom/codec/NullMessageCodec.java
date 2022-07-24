package com.obsidiandynamics.blackstrom.codec;

import com.obsidiandynamics.blackstrom.model.*;

public final class NullMessageCodec implements MessageCodec {
  @Override
  public byte[] encode(Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message decode(byte[] bytes) {
    throw new UnsupportedOperationException();
  }
}
