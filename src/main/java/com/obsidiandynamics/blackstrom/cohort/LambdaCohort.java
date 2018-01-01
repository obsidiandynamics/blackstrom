package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class LambdaCohort implements Cohort {
  @FunctionalInterface public interface OnInit {
    void onInit(InitContext context);
  }
  
  @FunctionalInterface public interface OnDispose {
    void onDispose();
  }
  
  private final OnInit onInit;
  
  private final OnDispose onDispose;
  
  private final NominationProcessor onNomination;
  
  private final OutcomeProcessor onOutcome;
  
  LambdaCohort(OnInit onInit, OnDispose onDispose, NominationProcessor onNomination,
               OutcomeProcessor onOutcome) {
    this.onInit = onInit;
    this.onDispose = onDispose;
    this.onNomination = onNomination;
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
  public void onNomination(MessageContext context, Nomination nomination) {
    onNomination.onNomination(context, nomination);
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    onOutcome.onOutcome(context, outcome);
  }
  
  public static LambdaCohortBuilder builder() {
    return new LambdaCohortBuilder();
  }
}
