package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class MessageHandlerAdapter implements MessageHandler {
  private final Factor factor;
  
  private final boolean queryCapable;
  private final boolean queryResponseCapable;
  private final boolean commandCapable;
  private final boolean commandResponseCapable;
  private final boolean noticeCapable;
  private final boolean proposalCapable;
  private final boolean voteCapable;
  private final boolean outcomeCapable;
  
  public MessageHandlerAdapter(Factor factor) {
    this.factor = factor;
    queryCapable = factor instanceof QueryProcessor;
    queryResponseCapable = factor instanceof QueryResponseProcessor;
    commandCapable = factor instanceof CommandProcessor;
    commandResponseCapable = factor instanceof CommandResponseProcessor;
    noticeCapable = factor instanceof NoticeProcessor;
    proposalCapable = factor instanceof ProposalProcessor;
    voteCapable = factor instanceof VoteProcessor;
    outcomeCapable = factor instanceof OutcomeProcessor;
  }

  @Override
  public void onMessage(MessageContext context, Message message) {
    switch (message.getMessageType()) {
      case QUERY:
        if (queryCapable) {
          ((QueryProcessor) factor).onQuery(context, (Query) message);
        }
        break;
        
      case QUERY_RESPONSE:
        if (queryResponseCapable) {
          ((QueryResponseProcessor) factor).onQueryResponse(context, (QueryResponse) message);
        }
        break;
        
      case COMMAND:
        if (commandCapable) {
          ((CommandProcessor) factor).onCommand(context, (Command) message);
        }
        break;
        
      case COMMAND_RESPONSE:
        if (commandResponseCapable) {
          ((CommandResponseProcessor) factor).onCommandResponse(context, (CommandResponse) message);
        }
        break;
        
      case NOTICE:
        if (noticeCapable) {
          ((NoticeProcessor) factor).onNotice(context, (Notice) message);
        }
        break;
        
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
        throw new UnsupportedOperationException("Unsupported message of type " + message.getMessageType());
    }
  }

  @Override
  public String getGroupId() {
    return factor.getGroupId();
  }
}
