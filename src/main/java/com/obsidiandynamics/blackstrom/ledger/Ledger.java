package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface Ledger extends Disposable.Default {
  default void init() {}
  
  void attach(MessageHandler handler);
  
  void append(Message message) throws Exception;
  
  void confirm(Object handlerId, Object messageId);
}
