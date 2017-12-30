package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface VoteHandler extends TypedMessageHandler {
  void onVote(MessageContext context, Vote vote);
}
