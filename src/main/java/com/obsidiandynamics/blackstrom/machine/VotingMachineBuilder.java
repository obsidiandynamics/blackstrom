package com.obsidiandynamics.blackstrom.machine;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;

public final class VotingMachineBuilder {
  private Ledger ledger;
  
  private final Set<Processor> processors = new HashSet<>();
  
  VotingMachineBuilder() {}
  
  public VotingMachineBuilder withLedger(Ledger ledger) {
    this.ledger = ledger;
    return this;
  }
  
  public VotingMachineBuilder withProcessors(Processor processor) {
    return withProcessors(processor);
  }
  
  public VotingMachineBuilder withProcessors(Processor... processors) {
    return withProcessors(Arrays.asList(processors));
  }
  
  public VotingMachineBuilder withProcessors(Collection<? extends Processor> processors) {
    this.processors.addAll(processors);
    return this;
  }
  
  public VotingMachine build() {
    return new VotingMachine(ledger, processors);
  }
}
