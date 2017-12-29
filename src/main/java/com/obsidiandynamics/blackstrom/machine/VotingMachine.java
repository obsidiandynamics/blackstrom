package com.obsidiandynamics.blackstrom.machine;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class VotingMachine implements Disposable {
  private final Ledger ledger;
  
  private final Set<Initiator> initiators = new CopyOnWriteArraySet<>();
  
  private final Set<Cohort> cohorts = new CopyOnWriteArraySet<>();
  
  private final Set<Monitor> monitors = new CopyOnWriteArraySet<>();
  
  VotingMachine(Ledger ledger, Collection<Initiator> initiators, Collection<Cohort> cohorts, Collection<Monitor> monitors) {
    this.ledger = ledger;
    this.initiators.addAll(initiators);
    this.cohorts.addAll(cohorts);
    this.monitors.addAll(monitors);
    
    initiators.forEach(i -> ledger.attach(new MessageHandlerAdapter(i)));
    cohorts.forEach(i -> ledger.attach(new MessageHandlerAdapter(i)));
    monitors.forEach(i -> ledger.attach(new MessageHandlerAdapter(i)));
    
    final InitContext context = new DefaultInitContext(ledger);
    initiators.forEach(i -> i.init(context));
    cohorts.forEach(i -> i.init(context));
    monitors.forEach(i -> i.init(context));    
    
    ledger.init(context);
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
    initiators.forEach(i -> i.dispose());
    cohorts.forEach(i -> i.dispose());
    monitors.forEach(i -> i.dispose());
  }
  
  public static VotingMachineBuilder builder() {
    return new VotingMachineBuilder();
  }
}
