package com.obsidiandynamics.blackstrom.machine;

import java.util.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class VotingMachineBuilder {
  private Ledger ledger;
  
  private final Set<Initiator> initiators = new HashSet<>();
  
  private final Set<Cohort> cohorts = new HashSet<>();
  
  private final Set<Monitor> monitors = new HashSet<>();
  
  VotingMachineBuilder() {}
  
  public VotingMachineBuilder withLedger(Ledger ledger) {
    this.ledger = ledger;
    return this;
  }
  
  public VotingMachineBuilder withInitiator(Initiator initiator) {
    return withInitiators(initiator);
  }
  
  public VotingMachineBuilder withInitiators(Initiator... initiators) {
    return withInitiators(Arrays.asList(initiators));
  }
  
  public VotingMachineBuilder withInitiators(Collection<? extends Initiator> initiators) {
    this.initiators.addAll(initiators);
    return this;
  }
  
  public VotingMachineBuilder withCohort(Cohort cohort) {
    return withCohorts(cohort);
  }
  
  public VotingMachineBuilder withCohorts(Cohort... cohorts) {
    return withCohorts(Arrays.asList(cohorts));
  }
  
  public VotingMachineBuilder withCohorts(Collection<? extends Cohort> cohorts) {
    this.cohorts.addAll(cohorts);
    return this;
  }
  
  public VotingMachineBuilder withMonitor(Monitor monitor) {
    return withMonitors(monitor);
  }
  
  public VotingMachineBuilder withMonitors(Monitor... monitors) {
    return withMonitors(Arrays.asList(monitors));
  }
  
  public VotingMachineBuilder withMonitors(Collection<? extends Monitor> monitors) {
    this.monitors.addAll(monitors);
    return this;
  }
  
  public VotingMachine build() {
    return new VotingMachine(ledger, initiators, cohorts, monitors);
  }
}
