package obsidiandynamics.blackstrom.handler;

import obsidiandynamics.blackstrom.model.*;

public interface DecisionHandler {
  void onDecision(VotingContext context, Decision decision);
}
