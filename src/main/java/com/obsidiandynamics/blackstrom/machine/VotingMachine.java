package com.obsidiandynamics.blackstrom.machine;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;

public final class VotingMachine implements Disposable {
  private final Ledger ledger;
  
  private final Set<Processor> processors;
  
  VotingMachine(Ledger ledger, Set<Processor> processors) {
    this.ledger = ledger;
    this.processors = processors;
    
    processors.forEach(p -> ledger.attach(new MessageHandlerAdapter(p)));
    ledger.init();
    
    final InitContext context = new DefaultInitContext(ledger);
    processors.forEach(p -> p.init(context));
  }
  
  public Ledger getLedger() {
    return ledger;
  }
  
  public Set<Processor> getHandlers() {
    return Collections.unmodifiableSet(processors);
  }

  @Override
  public void dispose() {
    ledger.dispose();
    processors.forEach(p -> p.dispose());
  }
  
  public static VotingMachineBuilder builder() {
    return new VotingMachineBuilder();
  }
}
