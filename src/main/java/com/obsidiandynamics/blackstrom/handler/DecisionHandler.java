package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public interface DecisionHandler {
  void onDecision(VotingContext context, Decision decision);
}
