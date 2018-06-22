package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class LambdaCohort implements Cohort {
  @FunctionalInterface public interface OnInit {
    void onInit(InitContext context);
  }
  
  @FunctionalInterface public interface OnDispose {
    void onDispose();
  }
  
  private final String groupId;
  
  private final OnInit onInit;
  
  private final OnDispose onDispose;
  
  private final QueryProcessor onQuery;
  
  private final CommandProcessor onCommand;
  
  private final NoticeProcessor onNotice;
  
  private final ProposalProcessor onProposal;
  
  private final VoteProcessor onVote;
  
  private final OutcomeProcessor onOutcome;
  
  LambdaCohort(String groupId, 
               OnInit onInit, 
               OnDispose onDispose, 
               QueryProcessor onQuery,
               CommandProcessor onCommand,
               NoticeProcessor onNotice,
               ProposalProcessor onProposal,
               VoteProcessor onVote,
               OutcomeProcessor onOutcome) {
    this.groupId = groupId;
    this.onInit = onInit;
    this.onDispose = onDispose;
    this.onQuery = onQuery;
    this.onCommand = onCommand;
    this.onNotice = onNotice;
    this.onProposal = onProposal;
    this.onVote = onVote;
    this.onOutcome = onOutcome;
  }

  @Override
  public void init(InitContext context) {
    onInit.onInit(context);
  }
  
  @Override
  public void dispose() {
    onDispose.onDispose();
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
  public void onCommand(MessageContext context, Command command) {
    onCommand.onCommand(context, command);
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
