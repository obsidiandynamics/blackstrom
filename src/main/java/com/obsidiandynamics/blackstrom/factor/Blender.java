package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

/**
 *  An adapter class that blends all supported handler interfaces into a single 
 *  abstract message handler compliant with {@link MessageTarget}.
 */
public abstract class Blender implements 
QueryProcessor, 
QueryResponseProcessor, 
CommandProcessor, 
CommandResponseProcessor,
NoticeProcessor, 
ProposalProcessor, 
VoteProcessor, 
OutcomeProcessor,
MessageTarget {
    
  @Override
  public final void onQuery(MessageContext context, Query query) {
    onMessage(context, query);
  }
  
  @Override
  public final void onQueryResponse(MessageContext context, QueryResponse queryResponse) {
    onMessage(context, queryResponse);
  }

  @Override
  public final void onCommand(MessageContext context, Command command) {
    onMessage(context, command);
  }

  @Override
  public final void onCommandResponse(MessageContext context, CommandResponse commandResponse) {
    onMessage(context, commandResponse);
  }

  @Override
  public final void onNotice(MessageContext context, Notice notice) {
    onMessage(context, notice);
  }

  @Override
  public final void onProposal(MessageContext context, Proposal proposal) {
    onMessage(context, proposal);
  }

  @Override
  public final void onVote(MessageContext context, Vote vote) {
    onMessage(context, vote);
  }

  @Override
  public final void onOutcome(MessageContext context, Outcome outcome) {
    onMessage(context, outcome);
  }
}
