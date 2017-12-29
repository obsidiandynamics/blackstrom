package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public interface DecisionHandler {
  void onDecision(MessageContext context, Decision decision);
}
