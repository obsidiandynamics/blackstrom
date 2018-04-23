package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface Ledger extends Disposable.Default {
  static AppendCallback sysErrAppendCallback = AppendCallback.errorLoggingAppendCallback(System.err);
  
  default void init() {}
  
  void attach(MessageHandler handler);
  
  void append(Message message, AppendCallback callback);
  
  default void append(Message message) {
    append(message, sysErrAppendCallback);
  }
  
  default void confirm(Object handlerId, MessageId messageId) {}
}
