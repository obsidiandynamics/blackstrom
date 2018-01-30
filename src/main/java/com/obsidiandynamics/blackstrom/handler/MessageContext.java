package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface MessageContext {
  Ledger getLedger();
  
  Object getHandlerId();
  
  default void publish(Message message) {
    getLedger().append(message);
  }
  
  default void confirm(Object messageId) {
    getLedger().confirm(getHandlerId(), messageId);
  }
}
