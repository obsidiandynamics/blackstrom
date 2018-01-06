package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.cohort.LambdaCohort.*;
import com.obsidiandynamics.blackstrom.handler.*;

public final class LambdaCohortBuilder {
  private String groupId;
  
  private OnInit onInit = c -> {};
  
  private OnDispose onDispose = () -> {};
  
  private NominationProcessor onNomination;
  
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

  public LambdaCohortBuilder onNomination(NominationProcessor onNomination) {
    this.onNomination = onNomination;
    return this;
  }

  public LambdaCohortBuilder onOutcome(OutcomeProcessor onOutcome) {
    this.onOutcome = onOutcome;
    return this;
  }
  
  public LambdaCohort build() {
    if (onNomination == null) throw new IllegalStateException("No onNomination behaviour set");
    if (onOutcome == null) throw new IllegalStateException("No onOutcome behaviour set");
    return new LambdaCohort(groupId, onInit, onDispose, onNomination, onOutcome);
  }
}
