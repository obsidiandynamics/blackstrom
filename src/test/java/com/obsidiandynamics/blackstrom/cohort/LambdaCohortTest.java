package com.obsidiandynamics.blackstrom.cohort;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.cohort.LambdaCohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class LambdaCohortTest {
  private static <M extends Message> void noOp(MessageContext context, M proposal) {}
  
  @Test(expected=IllegalStateException.class)
  public void testMissingOnProposal() {
    LambdaCohort.builder().onOutcome(LambdaCohortTest::noOp).build();
  }
  
  @Test(expected=IllegalStateException.class)
  public void testMissingOnOutcome() {
    LambdaCohort.builder().onProposal(LambdaCohortTest::noOp).build();
  }
  
  @Test
  public void testDefaultInitAndDispose() {
    final LambdaCohort l = LambdaCohort.builder()
        .onProposal(LambdaCohortTest::noOp)
        .onOutcome(LambdaCohortTest::noOp)
        .build();
    l.init(null);
    l.dispose();
  }
  
  @Test
  public void testGroupId() {
    final LambdaCohort l = LambdaCohort.builder()
        .withGroupId("test")
        .onProposal((c, m) -> {})
        .onOutcome((c, m) -> {})
        .build();
    
    assertEquals("test", l.getGroupId());
  }
  
  @Test
  public void testHandlers() {
    final OnInit onInit = mock(OnInit.class);
    final OnDispose onDispose = mock(OnDispose.class);
    final ProposalProcessor onProposal = mock(ProposalProcessor.class);
    final OutcomeProcessor onOutcome = mock(OutcomeProcessor.class);
    
    final InitContext initContext = mock(InitContext.class);
    final MessageContext messageContext = mock(MessageContext.class);
    final Proposal proposal = new Proposal(0, new String[0], null, 1000);
    final Outcome outcome = new Outcome(0, Verdict.COMMIT, null, new Response[0]);
    
    final LambdaCohort l = LambdaCohort.builder()
        .onInit(onInit)
        .onDispose(onDispose)
        .onProposal(onProposal)
        .onOutcome(onOutcome)
        .build();
    
    assertNull(l.getGroupId());
    
    l.init(initContext);
    verify(onInit).onInit(eq(initContext));
    
    l.onProposal(messageContext, proposal);
    verify(onProposal).onProposal(eq(messageContext), eq(proposal));
    
    l.onOutcome(messageContext, outcome);
    verify(onOutcome).onOutcome(eq(messageContext), eq(outcome));
    
    l.dispose();
    verify(onDispose).onDispose();
  }
}
