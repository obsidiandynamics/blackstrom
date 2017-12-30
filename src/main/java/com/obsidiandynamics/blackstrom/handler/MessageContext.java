package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface MessageContext {
  Ledger getLedger();
  
  default void vote(Object ballotId, String cohort, Pledge pledge, Object metadata) throws Exception {
    getLedger().append(new Vote(ballotId, new Response(cohort, pledge, metadata)));
  }
}