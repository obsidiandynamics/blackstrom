package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface DecisionHandler extends TypedMessageHandler {
  void onDecision(MessageContext context, Decision decision);
}
