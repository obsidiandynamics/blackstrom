package com.obsidiandynamics.blackstrom.cohort;

import static org.mockito.Mockito.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.cohort.LambdaCohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class LambdaCohortTest {
  private static <M extends Message> void noOp(MessageContext context, M nomination) {}
  
  @Test(expected=IllegalStateException.class)
  public void testMissingOnNomination() {
    LambdaCohort.builder().onOutcome(LambdaCohortTest::noOp).build();
  }
  
  @Test(expected=IllegalStateException.class)
  public void testMissingOnOutcome() {
    LambdaCohort.builder().onNomination(LambdaCohortTest::noOp).build();
  }
  
  @Test
  public void testDefaultInitAndDispose() {
    final LambdaCohort c = LambdaCohort.builder()
        .onNomination(LambdaCohortTest::noOp)
        .onOutcome(LambdaCohortTest::noOp)
        .build();
    c.init(null);
    c.dispose();
  }
  
  @Test
  public void testHandlers() {
    final OnInit onInit = mock(OnInit.class);
    final OnDispose onDispose = mock(OnDispose.class);
    final NominationProcessor onNomination = mock(NominationProcessor.class);
    final OutcomeProcessor onOutcome = mock(OutcomeProcessor.class);
    
    final InitContext initContext = mock(InitContext.class);
    final MessageContext messageContext = mock(MessageContext.class);
    final Nomination nomination = new Nomination(0, new String[0], null, 1000);
    final Outcome outcome = new Outcome(0, Verdict.COMMIT, new Response[0]);
    
    final LambdaCohort c = LambdaCohort.builder()
        .onInit(onInit)
        .onDispose(onDispose)
        .onNomination(onNomination)
        .onOutcome(onOutcome)
        .build();
    
    c.init(initContext);
    verify(onInit).onInit(eq(initContext));
    
    c.onNomination(messageContext, nomination);
    verify(onNomination).onNomination(eq(messageContext), eq(nomination));
    
    c.onOutcome(messageContext, outcome);
    verify(onOutcome).onOutcome(eq(messageContext), eq(outcome));
    
    c.dispose();
    verify(onDispose).onDispose();
  }
}
