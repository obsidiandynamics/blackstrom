package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class NonDisposableLedger implements Ledger {
  private final Ledger backing;
  
  public NonDisposableLedger(Ledger backing) {
    this.backing = backing;
  }

  @Override
  public void attach(MessageHandler handler) {
    backing.attach(handler);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    backing.append(message, callback);
  }

  @Override
  public void confirm(Object handlerId, MessageId messageId) {
    backing.confirm(handlerId, messageId);
  }

  @Override
  public boolean isAssigned(Object handlerId, int shard) {
    return backing.isAssigned(handlerId, shard);
  }

  @Override
  public void dispose() {}
}
