package com.obsidiandynamics.blackstrom.machine;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;

public final class VotingMachine implements Disposable {
  private final Ledger ledger;
  
  private final Set<Factor> factors;
  
  VotingMachine(Ledger ledger, Set<Factor> factors) {
    this.ledger = ledger;
    this.factors = factors;
    
    factors.forEach(f -> ledger.attach(new MessageHandlerAdapter(f)));
    ledger.init();
    
    final InitContext context = new DefaultInitContext(ledger);
    factors.forEach(f -> f.init(context));
  }
  
  public Ledger getLedger() {
    return ledger;
  }
  
  public Set<ElementalProcessor> getHandlers() {
    return Collections.unmodifiableSet(factors);
  }

  @Override
  public void dispose() {
    ledger.dispose();
    factors.forEach(p -> p.dispose());
  }
  
  public static VotingMachineBuilder builder() {
    return new VotingMachineBuilder();
  }
}
