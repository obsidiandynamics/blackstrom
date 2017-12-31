package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

public final class MessageHandlerAdapter implements MessageHandler {
  private final ElementalProcessor processor;
  
  private final boolean nominationCapable;
  private final boolean voteCapable;
  private final boolean outcomeCapable;
  
  public MessageHandlerAdapter(ElementalProcessor processor) {
    this.processor = processor;
    nominationCapable = processor instanceof NominationProcessor;
    voteCapable = processor instanceof VoteProcessor;
    outcomeCapable = processor instanceof OutcomeProcessor;
  }

  @Override
  public void onMessage(MessageContext context, Message message) {
    switch (message.getMessageType()) {
      case NOMINATION:
        if (nominationCapable) {
          ((NominationProcessor) processor).onNomination(context, (Nomination) message);
        }
        break;
        
      case VOTE:
        if (voteCapable) {
          ((VoteProcessor) processor).onVote(context, (Vote) message);
        }
        break;
        
      case OUTCOME:
        if (outcomeCapable) {
          ((OutcomeProcessor) processor).onOutcome(context, (Outcome) message);
        }
        break;
        
      default:
        throw new UnsupportedOperationException("Unsupported message of type " + message.getMessageType().name());
    }
  }
}
