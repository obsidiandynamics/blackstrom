package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface MessageContext {
  Ledger getLedger();
  
  default void vote(Object ballotId, String source, String cohort, Plea plea, Object metadata) throws Exception {
    getLedger().append(new Vote(ballotId, ballotId, source, new Response(cohort, plea, metadata)));
  }
}
