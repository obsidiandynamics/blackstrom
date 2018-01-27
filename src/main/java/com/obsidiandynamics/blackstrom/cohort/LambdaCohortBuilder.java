package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.cohort.LambdaCohort.*;
import com.obsidiandynamics.blackstrom.handler.*;

public final class LambdaCohortBuilder {
  private String groupId;
  
  private OnInit onInit = c -> {};
  
  private OnDispose onDispose = () -> {};
  
  private ProposalProcessor onProposal;
  
  private OutcomeProcessor onOutcome;
  
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

  public LambdaCohortBuilder onProposal(ProposalProcessor onProposal) {
    this.onProposal = onProposal;
    return this;
  }

  public LambdaCohortBuilder onOutcome(OutcomeProcessor onOutcome) {
    this.onOutcome = onOutcome;
    return this;
  }
  
  public LambdaCohort build() {
    if (onProposal == null) throw new IllegalStateException("No onProposal behaviour set");
    if (onOutcome == null) throw new IllegalStateException("No onOutcome behaviour set");
    return new LambdaCohort(groupId, onInit, onDispose, onProposal, onOutcome);
  }
}
