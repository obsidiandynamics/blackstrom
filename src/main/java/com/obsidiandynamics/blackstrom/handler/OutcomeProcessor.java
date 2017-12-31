package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface OutcomeProcessor extends Processor {
  void onOutcome(MessageContext context, Outcome outcome);
}
