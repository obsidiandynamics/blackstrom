package com.obsidiandynamics.blackstrom.initiator;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class AsyncInitiator implements Initiator {
  private final String source;
  
  private final Supplier<Object> idGenerator;
  
  private final Map<Object, Consumer<Decision>> pending = new ConcurrentHashMap<>();
  
  private VotingMachine machine;
  
  public AsyncInitiator(String source) {
    this(source, UUID::randomUUID);
  }

  public AsyncInitiator(String source, Supplier<Object> idGenerator) {
    this.source = source;
    this.idGenerator = idGenerator;
  }
  
  @Override
  public void init(VotingMachine machine) {
    this.machine = machine;
  }
  
  public CompletableFuture<Decision> initiate(String[] cohorts, Object proposal, int ttl) throws Exception {
    final CompletableFuture<Decision> f = new CompletableFuture<>();
    initiate(cohorts, proposal, ttl, f::complete);
    return f;
  }
  
  public void initiate(String[] cohorts, Object proposal, int ttl, Consumer<Decision> callback) throws Exception {
    final Object ballotId = idGenerator.get();
    pending.put(ballotId, callback);
    machine.getLedger().append(new Nomination(ballotId, ballotId, source, cohorts, proposal, ttl));
  }

  @Override
  public void onDecision(VotingContext context, Decision decision) {
    final Consumer<Decision> callback = pending.remove(decision.getBallotId());
    if (callback != null) {
      callback.accept(decision);
    }
  }
}
