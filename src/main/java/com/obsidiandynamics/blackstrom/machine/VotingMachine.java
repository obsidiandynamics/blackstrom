package com.obsidiandynamics.blackstrom.machine;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;

public final class VotingMachine implements Disposable {
  private final Ledger ledger;
  
  private final Set<TypedMessageHandler> handlers;
  
  VotingMachine(Ledger ledger, Set<TypedMessageHandler> handlers) {
    this.ledger = ledger;
    this.handlers = handlers;
    
    handlers.forEach(h -> ledger.attach(h.toUntypedHandler()));
    ledger.init();
    
    final InitContext context = new DefaultInitContext(ledger);
    handlers.forEach(h -> h.init(context));
  }
  
  public Ledger getLedger() {
    return ledger;
  }
  
  public Set<TypedMessageHandler> getHandlers() {
    return Collections.unmodifiableSet(handlers);
  }

  @Override
  public void dispose() {
    ledger.dispose();
    handlers.forEach(h -> h.dispose());
  }
  
  public static VotingMachineBuilder builder() {
    return new VotingMachineBuilder();
  }
}
