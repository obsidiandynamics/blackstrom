package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface AppendCallback {
  static AppendCallback nop() { return (__id, __error) -> {}; }
  
  void onAppend(MessageId messageId, Throwable error);
}
