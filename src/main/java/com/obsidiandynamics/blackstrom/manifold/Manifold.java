package com.obsidiandynamics.blackstrom.manifold;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;

public final class Manifold implements Disposable {
  private final Ledger ledger;
  
  private final Set<Factor> factors;
  
  Manifold(Ledger ledger, Set<Factor> factors) {
    this.ledger = ledger;
    this.factors = factors;
    
    final InitContext context = new DefaultInitContext(ledger);
    factors.forEach(f -> f.init(context));
    
    factors.forEach(f -> ledger.attach(new MessageHandlerAdapter(f)));
    ledger.init();
  }
  
  public Ledger getLedger() {
    return ledger;
  }
  
  public Set<Factor> getFactors() {
    return Collections.unmodifiableSet(factors);
  }

  @Override
  public void dispose() {
    ledger.dispose();
    factors.forEach(Disposable::dispose);
  }
  
  public static ManifoldBuilder builder() {
    return new ManifoldBuilder();
  }
}
