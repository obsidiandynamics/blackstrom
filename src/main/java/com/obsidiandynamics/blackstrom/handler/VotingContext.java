package com.obsidiandynamics.blackstrom.handler;

import java.util.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface VotingContext {
  Ledger getLedger();
  
  default Object nextMessageId() {
    return UUID.randomUUID();
  }
  
  default void vote(Object ballotId, String source, String cohort, Plea plea, Object metadata) throws Exception {
    getLedger().append(new Vote(nextMessageId(), ballotId, source, new Response(cohort, plea, metadata)));
  }
}
