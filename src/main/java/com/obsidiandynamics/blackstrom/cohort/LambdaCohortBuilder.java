package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.cohort.LambdaCohort.*;
import com.obsidiandynamics.blackstrom.factor.*;

public final class LambdaCohortBuilder {
  private String groupId;
  
  private OnInit onInit = c -> {};
  
  private OnDispose onDispose = () -> {};
  
  private QueryProcessor onQuery = (__context, __query) -> {};
  
  private CommandProcessor onCommand = (__context, __command) -> {};
  
  private NoticeProcessor onNotice = (__context, __notice) -> {};
  
  private ProposalProcessor onProposal = (__context, __proposal) -> {};
  
  private VoteProcessor onVote = (__context, __vote) -> {};
  
  private OutcomeProcessor onOutcome = (__context, __outcome) -> {};
  
  LambdaCohortBuilder() {}
  
  public LambdaCohortBuilder withGroupId(String groupId) {
    this.groupId = groupId;
    return this;
  }

  public LambdaCohortBuilder onInit(OnInit onInit) {
    this.onInit = onInit;
    return this;
  }

  public LambdaCohortBuilder onDispose(OnDispose onDispose) {
    this.onDispose = onDispose;
    return this;
  }

  public LambdaCohortBuilder onQuery(QueryProcessor onQuery) {
    this.onQuery = onQuery;
    return this;
  }

  public LambdaCohortBuilder onCommand(CommandProcessor onCommand) {
    this.onCommand = onCommand;
    return this;
  }

  public LambdaCohortBuilder onNotice(NoticeProcessor onNotice) {
    this.onNotice = onNotice;
    return this;
  }

  public LambdaCohortBuilder onProposal(ProposalProcessor onProposal) {
    this.onProposal = onProposal;
    return this;
  }

  public LambdaCohortBuilder onVote(VoteProcessor onVote) {
    this.onVote = onVote;
    return this;
  }

  public LambdaCohortBuilder onOutcome(OutcomeProcessor onOutcome) {
    this.onOutcome = onOutcome;
    return this;
  }
  
  public LambdaCohort build() {
    return new LambdaCohort(groupId, onInit, onDispose, onQuery, onCommand, onNotice, onProposal, onVote, onOutcome);
  }
}
