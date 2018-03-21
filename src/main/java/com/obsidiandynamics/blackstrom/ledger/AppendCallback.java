package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface AppendCallback {
  void onAppend(MessageId messageId, Throwable error);
}
