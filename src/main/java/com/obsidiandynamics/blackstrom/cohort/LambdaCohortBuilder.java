package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.factor.*;

public final class LambdaCohortBuilder {
  private static final Initable DEF_ON_INIT = new Initable.Nop() {};
  private static final Disposable DEF_ON_DISPOSE = new Disposable.Nop() {};
  private static final QueryProcessor DEF_ON_QUERY = new QueryProcessor.Nop() {};
  private static final QueryResponseProcessor DEF_ON_QUERY_RESPONSE = new QueryResponseProcessor.Nop() {};
  private static final CommandProcessor DEF_ON_COMMAND = new CommandProcessor.Nop() {};
  private static final CommandResponseProcessor DEF_ON_COMMAND_RESPONSE = new CommandResponseProcessor.Nop() {};
  private static final NoticeProcessor DEF_ON_NOTICE = new NoticeProcessor.Nop() {};
  private static final ProposalProcessor DEF_ON_PROPOSAL = new ProposalProcessor.Nop() {};
  private static final VoteProcessor DEF_ON_VOTE = new VoteProcessor.Nop() {};
  private static final OutcomeProcessor DEF_ON_OUTCOME = new OutcomeProcessor.Nop() {};
  
  private String groupId;
  
  private Initable onInit = DEF_ON_INIT;
  
  private Disposable onDispose = DEF_ON_DISPOSE;
  
  private QueryProcessor onQuery = DEF_ON_QUERY;

  private QueryResponseProcessor onQueryResponse = DEF_ON_QUERY_RESPONSE;
  
  private CommandProcessor onCommand = DEF_ON_COMMAND;

  private CommandResponseProcessor onCommandResponse = DEF_ON_COMMAND_RESPONSE;
  
  private NoticeProcessor onNotice = DEF_ON_NOTICE;
  
  private ProposalProcessor onProposal = DEF_ON_PROPOSAL;
  
  private VoteProcessor onVote = DEF_ON_VOTE;
  
  private OutcomeProcessor onOutcome = DEF_ON_OUTCOME;
  
  LambdaCohortBuilder() {}
  
  public LambdaCohortBuilder withGroupId(String groupId) {
    this.groupId = groupId;
    return this;
  }

  public LambdaCohortBuilder onInit(Initable onInit) {
    this.onInit = onInit;
    return this;
  }

  public LambdaCohortBuilder onDispose(Disposable onDispose) {
    this.onDispose = onDispose;
    return this;
  }

  public LambdaCohortBuilder onQuery(QueryProcessor onQuery) {
    this.onQuery = onQuery;
    return this;
  }

  public LambdaCohortBuilder onQueryResponse(QueryResponseProcessor onQueryResponse) {
    this.onQueryResponse = onQueryResponse;
    return this;
  }

  public LambdaCohortBuilder onCommand(CommandProcessor onCommand) {
    this.onCommand = onCommand;
    return this;
  }

  public LambdaCohortBuilder onCommandResponse(CommandResponseProcessor onCommandResponse) {
    this.onCommandResponse = onCommandResponse;
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
    return new LambdaCohort(groupId, onInit, onDispose, onQuery, onQueryResponse, onCommand, onCommandResponse,
                            onNotice, onProposal, onVote, onOutcome);
  }
}
