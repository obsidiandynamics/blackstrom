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
import com.obsidiandynamics.zerolog.*;

public final class AsyncInitiator implements Initiator, NullGroup, Disposable.Nop {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private final Map<String, Consumer<?>> pending = new ConcurrentHashMap<>();

  private int logLevel = LogLevel.TRACE; 
  
  private Ledger ledger;
  
  public AsyncInitiator withLogLevel(int logLevel) {
    this.logLevel = logLevel;
    return this;
  }
  
  @Override
  public void init(InitContext context) {
    this.ledger = context.getLedger();
  }
  
  public CompletableFuture<QueryResponse> initiate(Query query) {
    return initiate(query, Ledger.getDefaultAppendCallback());
  }
  
  public CompletableFuture<QueryResponse> initiate(Query query, 
                                                   AppendCallback appendCallback) {
    return genericInitiate(query, appendCallback);
  }
  
  public CompletableFuture<CommandResponse> initiate(Command command) {
    return initiate(command, Ledger.getDefaultAppendCallback());
  }
  
  public CompletableFuture<CommandResponse> initiate(Command command, 
                                                     AppendCallback appendCallback) {
    return genericInitiate(command, appendCallback);
  }
  
  public CompletableFuture<Outcome> initiate(Proposal proposal) {
    return initiate(proposal, Ledger.getDefaultAppendCallback());
  }
  
  public CompletableFuture<Outcome> initiate(Proposal proposal, 
                                             AppendCallback appendCallback) {
    return genericInitiate(proposal, appendCallback);
  }
  
  private <REQ extends Message, RES extends Message> CompletableFuture<RES> genericInitiate(REQ request, 
                                                                                            AppendCallback appendCallback) {
    final var f = new CompletableFuture<RES>();
    this.genericInitiate(request, f::complete, appendCallback);
    return f;
  }
  
  public void initiate(Query query, 
                       Consumer<? super QueryResponse> responseCallback) {
    initiate(query, responseCallback, Ledger.getDefaultAppendCallback());
  }
  
  public void initiate(Query query, 
                       Consumer<? super QueryResponse> responseCallback, 
                       AppendCallback appendCallback) {
    this.<Query, QueryResponse>genericInitiate(query, responseCallback, appendCallback);
  }
  
  public void initiate(Command command, 
                       Consumer<? super CommandResponse> responseCallback) {
    initiate(command, responseCallback, Ledger.getDefaultAppendCallback());
  }
  
  public void initiate(Command command, 
                       Consumer<? super CommandResponse> responseCallback, 
                       AppendCallback appendCallback) {
    this.<Command, CommandResponse>genericInitiate(command, responseCallback, appendCallback);
  }
  
  public void initiate(Proposal proposal, 
                       Consumer<? super Outcome> responseCallback) {
    initiate(proposal, responseCallback, Ledger.getDefaultAppendCallback());
  }
  
  public void initiate(Proposal proposal, 
                       Consumer<? super Outcome> responseCallback, 
                       AppendCallback appendCallback) {
    this.<Proposal, Outcome>genericInitiate(proposal, responseCallback, appendCallback);
  }
  
  public boolean isPending(Message message) {
    return isPending(message.getXid());
  }
  
  public boolean isPending(String xid) {
    return pending.containsKey(xid);
  }
  
  public void cancel(Message message) {
    cancel(message.getXid());
  }
  
  public void cancel(String xid) {
    pending.remove(xid);
  }
  
  private <REQ extends Message, RES extends Message> void genericInitiate(REQ message, 
                                                                          Consumer<? super RES> responseCallback, 
                                                                          AppendCallback appendCallback) {
    pending.put(message.getXid(), responseCallback);
    ledger.append(message, appendCallback);
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
    final var callback = pending.remove(message.getXid());
    if (callback != null) {
      zlg.level(logLevel).format("Matched response %s").arg(message).log();
      callback.accept(Classes.cast(message));
    } else {
      zlg.level(logLevel).format("Unmatched response %s").arg(message).log();
    }
  }
}
