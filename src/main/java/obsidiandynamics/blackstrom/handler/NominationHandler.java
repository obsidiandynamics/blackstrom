package obsidiandynamics.blackstrom.handler;

import obsidiandynamics.blackstrom.model.*;

public interface NominationHandler {
  void onNomination(VotingContext context, Nomination nomination);
}
