package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public interface MessageHandler {
  void onMessage(VotingContext context, Message message);
}
