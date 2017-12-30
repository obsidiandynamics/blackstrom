package com.obsidiandynamics.blackstrom.machine;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class VotingMachine implements Disposable {
  private final Ledger ledger;
  
  private final Set<Initiator> initiators = new HashSet<>();
  
  private final Set<Cohort> cohorts = new HashSet<>();
  
  private final Set<Monitor> monitors = new HashSet<>();
  
  private final Set<TypedMessageHandler> allHandlers = new HashSet<>();
  
  VotingMachine(Ledger ledger, Collection<Initiator> initiators, Collection<Cohort> cohorts, Collection<Monitor> monitors) {
    this.ledger = ledger;
    this.initiators.addAll(initiators);
    this.cohorts.addAll(cohorts);
    this.monitors.addAll(monitors);
    
    allHandlers.addAll(initiators);
    allHandlers.addAll(cohorts);
    allHandlers.addAll(monitors);
    
    allHandlers.forEach(h -> ledger.attach(h.toUntypedHandler()));
    
    final InitContext context = new DefaultInitContext(ledger);
    allHandlers.forEach(h -> h.init(context));
    
    ledger.init();
  }
  
  public Ledger getLedger() {
    return ledger;
  }
  
  public Set<Initiator> getInitiators() {
    return Collections.unmodifiableSet(initiators);
  }
  
  public Set<Cohort> getCohorts() {
    return Collections.unmodifiableSet(cohorts);
  }
  
  public Set<Monitor> getMonitors() {
    return Collections.unmodifiableSet(monitors);
  }

  @Override
  public void dispose() {
    ledger.dispose();
    allHandlers.forEach(h -> h.dispose());
  }
  
  public static VotingMachineBuilder builder() {
    return new VotingMachineBuilder();
  }
}
