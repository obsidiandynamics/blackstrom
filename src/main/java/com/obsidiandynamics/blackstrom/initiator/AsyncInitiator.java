package com.obsidiandynamics.blackstrom.initiator;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class AsyncInitiator implements Initiator {
  private final String source;
  
  private final Map<Object, Consumer<Decision>> pending = new ConcurrentHashMap<>();
  
  private Ledger ledger;
  
  public AsyncInitiator(String source) {
    this.source = source;
  }
  
  @Override
  public void init(InitContext context) {
    this.ledger = context.getLedger();
  }
  
  public CompletableFuture<Decision> initiate(Object ballotId, String[] cohorts, Object proposal, int ttl) throws Exception {
    final CompletableFuture<Decision> f = new CompletableFuture<>();
    initiate(ballotId, cohorts, proposal, ttl, f::complete);
    return f;
  }
  
  public void initiate(Object ballotId, String[] cohorts, Object proposal, int ttl, Consumer<Decision> callback) throws Exception {
    pending.put(ballotId, callback);
    ledger.append(new Nomination(ballotId, ballotId, source, cohorts, proposal, ttl));
  }

  @Override
  public void onDecision(MessageContext context, Decision decision) {
    final Consumer<Decision> callback = pending.remove(decision.getBallotId());
    if (callback != null) {
      callback.accept(decision);
    }
  }
}
