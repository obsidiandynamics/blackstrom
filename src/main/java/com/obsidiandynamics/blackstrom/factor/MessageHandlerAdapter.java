package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class MessageHandlerAdapter implements MessageHandler {
  private final Factor factor;
  
  private final boolean proposalCapable;
  private final boolean voteCapable;
  private final boolean outcomeCapable;
  
  public MessageHandlerAdapter(Factor factor) {
    this.factor = factor;
    proposalCapable = factor instanceof ProposalProcessor;
    voteCapable = factor instanceof VoteProcessor;
    outcomeCapable = factor instanceof OutcomeProcessor;
  }

  @Override
  public void onMessage(MessageContext context, Message message) {
    switch (message.getMessageType()) {
      case PROPOSAL:
        if (proposalCapable) {
          ((ProposalProcessor) factor).onProposal(context, (Proposal) message);
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
        
      case $UNKNOWN:
      default:
        throw new UnsupportedOperationException("Unsupported message of type " + message.getMessageType().name());
    }
  }

  @Override
  public String getGroupId() {
    return factor.getGroupId();
  }
}
