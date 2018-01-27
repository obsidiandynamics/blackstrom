package com.obsidiandynamics.blackstrom.manifold;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;

public final class ManifoldBuilder {
  private Ledger ledger;
  
  private final Set<Factor> factors = new HashSet<>();
  
  ManifoldBuilder() {}
  
  public ManifoldBuilder withLedger(Ledger ledger) {
    this.ledger = ledger;
    return this;
  }
  
  public ManifoldBuilder withFactors(Factor... factors) {
    return withFactors(Arrays.asList(factors));
  }
  
  public ManifoldBuilder withFactors(Collection<? extends Factor> factors) {
    this.factors.addAll(factors);
    return this;
  }
  
  public Manifold build() {
    return new Manifold(ledger, factors);
  }
}
