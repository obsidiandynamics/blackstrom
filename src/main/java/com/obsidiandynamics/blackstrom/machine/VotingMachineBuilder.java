package com.obsidiandynamics.blackstrom.machine;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;

public final class VotingMachineBuilder {
  private Ledger ledger;
  
  private final Set<Factor> factors = new HashSet<>();
  
  VotingMachineBuilder() {}
  
  public VotingMachineBuilder withLedger(Ledger ledger) {
    this.ledger = ledger;
    return this;
  }
  
  public VotingMachineBuilder withFactors(Factor... factors) {
    return withFactors(Arrays.asList(factors));
  }
  
  public VotingMachineBuilder withFactors(Collection<? extends Factor> factors) {
    this.factors.addAll(factors);
    return this;
  }
  
  public VotingMachine build() {
    return new VotingMachine(ledger, factors);
  }
}
