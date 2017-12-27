package obsidiandynamics.blackstrom.handler;

import obsidiandynamics.blackstrom.model.*;

public interface MessageHandler {
  void onMessage(VotingContext context, Message message);
}
