package com.obsidiandynamics.blackstrom.initiator;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.handler.Groupable.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.func.*;

public final class AsyncInitiator implements Initiator, NullGroup, Disposable.Nop {
  private final Map<String, Consumer<?>> pending = new ConcurrentHashMap<>();
  
  private Ledger ledger;
  
  @Override
  public void init(InitContext context) {
    this.ledger = context.getLedger();
  }
  
  public CompletableFuture<QueryResponse> initiate(Query query) {
    return genericInitiate(query);
  }
  
  public CompletableFuture<CommandResponse> initiate(Command command) {
    return genericInitiate(command);
  }
  
  public CompletableFuture<Outcome> initiate(Proposal proposal) {
    return genericInitiate(proposal);
  }
  
  private <REQ extends Message, RES extends Message> CompletableFuture<RES> genericInitiate(REQ request) {
    final CompletableFuture<RES> f = new CompletableFuture<>();
    this.genericInitiate(request, f::complete);
    return f;
  }
  
  public void initiate(Query query, Consumer<? super QueryResponse> callback) {
    this.<Query, QueryResponse>genericInitiate(query, callback);
  }
  
  public void initiate(Command command, Consumer<? super CommandResponse> callback) {
    this.<Command, CommandResponse>genericInitiate(command, callback);
  }
  
  public void initiate(Proposal proposal, Consumer<? super Outcome> callback) {
    this.<Proposal, Outcome>genericInitiate(proposal, callback);
  }
  
  private <REQ extends Message, RES extends Message> void genericInitiate(REQ message, Consumer<? super RES> callback) {
    pending.put(message.getXid(), callback);
    ledger.append(message);
  }

  @Override
  public void onQueryResponse(MessageContext context, QueryResponse queryResponse) {
    onMessage(context, queryResponse);
  }

  @Override
  public void onCommandResponse(MessageContext context, CommandResponse commandResponse) {
    onMessage(context, commandResponse);
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    onMessage(context, outcome);
  }
  
  private void onMessage(MessageContext context, Message message) {
    final Consumer<?> callback = pending.remove(message.getXid());
    if (callback != null) {
      callback.accept(Classes.cast(message));
    }
  }
}
