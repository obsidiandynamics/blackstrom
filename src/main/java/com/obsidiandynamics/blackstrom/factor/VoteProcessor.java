package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface VoteProcessor extends ElementalProcessor {
  void onVote(MessageContext context, Vote vote);
  
  interface Nop extends VoteProcessor {
    @Override 
    default void onVote(MessageContext context, Vote vote) {}
  }
  
  interface BeginAndConfirm extends VoteProcessor {
    @Override 
    default void onVote(MessageContext context, Vote vote) {
      context.beginAndConfirm(vote);
    }
  }
}
