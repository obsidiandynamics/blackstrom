package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface OutcomeProcessor extends ElementalProcessor {
  void onOutcome(MessageContext context, Outcome outcome);
  
  interface Nop extends OutcomeProcessor {
    @Override 
    default void onOutcome(MessageContext context, Outcome outcome) {}
  }
  
  interface BeginAndConfirm extends OutcomeProcessor {
    @Override 
    default void onOutcome(MessageContext context, Outcome outcome) {
      context.beginAndConfirm(outcome);
    }
  }
}
