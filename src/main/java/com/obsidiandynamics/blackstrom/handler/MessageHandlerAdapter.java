package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public final class MessageHandlerAdapter implements MessageHandler {
  private final Object handler;
  
  private final boolean nominationCapable;
  private final boolean voteCapable;
  private final boolean decisionCapable;
  
  public MessageHandlerAdapter(Object handler) {
    this.handler = handler;
    nominationCapable = handler instanceof NominationHandler;
    voteCapable = handler instanceof VoteHandler;
    decisionCapable = handler instanceof DecisionHandler;
  }

  @Override
  public void onMessage(VotingContext context, Message message) {
    switch (message.getMessageType()) {
      case NOMINATION:
        if (nominationCapable) {
          ((NominationHandler) handler).onNomination(context, (Nomination) message);
        }
        break;
        
      case VOTE:
        if (voteCapable) {
          ((VoteHandler) handler).onVote(context, (Vote) message);
        }
        break;
        
      case DECISION:
        if (decisionCapable) {
          ((DecisionHandler) handler).onDecision(context, (Decision) message);
        }
        break;
        
      default:
        throw new UnsupportedOperationException("Unsupported message of type " + message.getMessageType().name());
    }
  }
}
