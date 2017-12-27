package com.obsidiandynamics.blackstrom.ledger.direct;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class DirectLedger implements Ledger {
  private final List<MessageHandler> handlers = new ArrayList<>();
  
  private final VotingContext context = new DefaultVotingContext(this);
  
  @Override
  public void attach(MessageHandler handler) {
    handlers.add(handler);
  }

  @Override
  public void append(Message message) throws Exception {
    for (int i = handlers.size(); --i >= 0; ) {
      handlers.get(i).onMessage(context, message);
    }
  }
}
