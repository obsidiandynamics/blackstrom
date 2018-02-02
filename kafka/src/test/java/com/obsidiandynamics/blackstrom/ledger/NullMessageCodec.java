package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;

final class NullMessageCodec implements MessageCodec {
  @Override
  public byte[] encode(Message message) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message decode(byte[] bytes) throws Exception {
    throw new UnsupportedOperationException();
  }
}
