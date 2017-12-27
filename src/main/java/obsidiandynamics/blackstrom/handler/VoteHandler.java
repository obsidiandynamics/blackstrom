package obsidiandynamics.blackstrom.handler;

import obsidiandynamics.blackstrom.model.*;

public interface VoteHandler {
  void onVote(VotingContext context, Vote vote);
}
