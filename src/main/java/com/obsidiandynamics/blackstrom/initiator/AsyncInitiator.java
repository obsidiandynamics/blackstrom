package com.obsidiandynamics.blackstrom.initiator;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.handler.Groupable.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class AsyncInitiator implements Initiator, NullGroup {
  private final Map<Object, Consumer<Outcome>> pending = new ConcurrentHashMap<>();
  
  private Ledger ledger;
  
  @Override
  public void init(InitContext context) {
    this.ledger = context.getLedger();
  }
  
  public CompletableFuture<Outcome> initiate(Proposal proposal) {
    final CompletableFuture<Outcome> f = new CompletableFuture<>();
    initiate(proposal, f::complete);
    return f;
  }
  
  public void initiate(Proposal proposal, Consumer<Outcome> callback) {
    pending.put(proposal.getXid(), callback);
    ledger.append(proposal);
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    final Consumer<Outcome> callback = pending.remove(outcome.getXid());
    if (callback != null) {
      callback.accept(outcome);
    }
  }
}
