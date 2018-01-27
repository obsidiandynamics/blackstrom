package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface OutcomeProcessor extends ElementalProcessor {
  void onOutcome(MessageContext context, Outcome outcome);
}
