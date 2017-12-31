package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface VoteProcessor extends ElementalProcessor {
  void onVote(MessageContext context, Vote vote);
}
