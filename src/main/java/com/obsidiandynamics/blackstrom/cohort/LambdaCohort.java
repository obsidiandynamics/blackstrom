package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class LambdaCohort implements Cohort {
  private final String groupId;
  
  private final Initable onInit;
  
  private final Disposable onDispose;
  
  private final QueryProcessor onQuery;

  private final QueryResponseProcessor onQueryResponse;
  
  private final CommandProcessor onCommand;

  private final CommandResponseProcessor onCommandResponse;
  
  private final NoticeProcessor onNotice;
  
  private final ProposalProcessor onProposal;
  
  private final VoteProcessor onVote;
  
  private final OutcomeProcessor onOutcome;
  
  LambdaCohort(String groupId, 
               Initable onInit, 
               Disposable onDispose, 
               QueryProcessor onQuery,
               QueryResponseProcessor onQueryResponse,
               CommandProcessor onCommand,
               CommandResponseProcessor onCommandResponse,
               NoticeProcessor onNotice,
               ProposalProcessor onProposal,
               VoteProcessor onVote,
               OutcomeProcessor onOutcome) {
    this.groupId = groupId;
    this.onInit = onInit;
    this.onDispose = onDispose;
    this.onQuery = onQuery;
    this.onQueryResponse = onQueryResponse;
    this.onCommand = onCommand;
    this.onCommandResponse = onCommandResponse;
    this.onNotice = onNotice;
    this.onProposal = onProposal;
    this.onVote = onVote;
    this.onOutcome = onOutcome;
  }

  @Override
  public void init(InitContext context) {
    onInit.init(context);
  }
  
  @Override
  public void dispose() {
    onDispose.dispose();
  }
  
  @Override
  public String getGroupId() {
    return groupId;
  }

  @Override
  public void onQuery(MessageContext context, Query query) {
    onQuery.onQuery(context, query);
  }

  @Override
  public void onQueryResponse(MessageContext context, QueryResponse queryResponse) {
    onQueryResponse.onQueryResponse(context, queryResponse);
  }

  @Override
  public void onCommand(MessageContext context, Command command) {
    onCommand.onCommand(context, command);
  }

  @Override
  public void onCommandResponse(MessageContext context, CommandResponse commandResponse) {
    onCommandResponse.onCommandResponse(context, commandResponse);
  }

  @Override
  public void onNotice(MessageContext context, Notice notice) {
    onNotice.onNotice(context, notice);
  }
  
  @Override
  public void onProposal(MessageContext context, Proposal proposal) {
    onProposal.onProposal(context, proposal);
  }

  @Override
  public void onVote(MessageContext context, Vote vote) {
    onVote.onVote(context, vote);
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    onOutcome.onOutcome(context, outcome);
  }
  
  public static LambdaCohortBuilder builder() {
    return new LambdaCohortBuilder();
  }
}
