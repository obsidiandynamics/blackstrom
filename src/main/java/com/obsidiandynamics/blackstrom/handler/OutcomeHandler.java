package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface OutcomeHandler extends TypedMessageHandler {
  void onOutcome(MessageContext context, Outcome outcome);
}
