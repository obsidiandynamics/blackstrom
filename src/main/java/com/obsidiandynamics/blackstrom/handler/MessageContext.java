package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface MessageContext {
  Ledger getLedger();
  
  Object getHandlerId();
  
  default void vote(Object ballotId, String cohort, Pledge pledge, Object metadata) {
    getLedger().append(new Vote(ballotId, new Response(cohort, pledge, metadata)));
  }
  
  default void confirm(Object messageId) {
    getLedger().confirm(getHandlerId(), messageId);
  }
}
