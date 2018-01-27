package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface MessageContext {
  Ledger getLedger();
  
  Object getHandlerId();
  
  default void vote(Object ballotId, String cohort, Intent intent, Object metadata) {
    getLedger().append(new Vote(ballotId, new Response(cohort, intent, metadata)));
  }
  
  default void confirm(Object messageId) {
    getLedger().confirm(getHandlerId(), messageId);
  }
}
