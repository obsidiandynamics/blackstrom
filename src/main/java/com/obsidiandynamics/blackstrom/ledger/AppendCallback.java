package com.obsidiandynamics.blackstrom.ledger;

@FunctionalInterface
public interface AppendCallback {
  void onAppend(Object messageId, Exception exception);
}
