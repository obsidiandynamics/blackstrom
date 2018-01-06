package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public final class MessageHandlerAdapter implements MessageHandler {
  private final Factor factor;
  
  private final boolean nominationCapable;
  private final boolean voteCapable;
  private final boolean outcomeCapable;
  
  public MessageHandlerAdapter(Factor factor) {
    this.factor = factor;
    nominationCapable = factor instanceof NominationProcessor;
    voteCapable = factor instanceof VoteProcessor;
    outcomeCapable = factor instanceof OutcomeProcessor;
  }

  @Override
  public void onMessage(MessageContext context, Message message) {
    switch (message.getMessageType()) {
      case NOMINATION:
        if (nominationCapable) {
          ((NominationProcessor) factor).onNomination(context, (Nomination) message);
        }
        break;
        
      case VOTE:
        if (voteCapable) {
          ((VoteProcessor) factor).onVote(context, (Vote) message);
        }
        break;
        
      case OUTCOME:
        if (outcomeCapable) {
          ((OutcomeProcessor) factor).onOutcome(context, (Outcome) message);
        }
        break;
        
      default:
        throw new UnsupportedOperationException("Unsupported message of type " + message.getMessageType().name());
    }
  }

  @Override
  public String getGroupId() {
    return factor.getGroupId();
  }
}
