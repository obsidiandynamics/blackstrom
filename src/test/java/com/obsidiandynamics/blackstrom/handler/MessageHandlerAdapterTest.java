package com.obsidiandynamics.blackstrom.handler;

import static junit.framework.TestCase.*;

import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class MessageHandlerAdapterTest {
  interface ProposalFactor extends Factor, ProposalProcessor, Groupable.NullGroup {};
  
  interface VoteFactor extends Factor, VoteProcessor, Groupable.NullGroup {};
  
  interface OutcomeFactor extends Factor, OutcomeProcessor, Groupable.NullGroup {};
  
  @Test
  public void testProposalAndGroup() {
    final AtomicInteger invocations = new AtomicInteger();
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new ProposalFactor() {
      @Override public void onProposal(MessageContext context, Proposal proposal) {
        invocations.incrementAndGet();
      }
      
      @Override public String getGroupId() {
        return "test-proposal";
      }
    });
    callAll(adapter);
    assertEquals(1, invocations.get());
    assertEquals("test-proposal", adapter.getGroupId());
  }

  @Test
  public void testVote() {
    final AtomicInteger invocations = new AtomicInteger();
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new VoteFactor() {
      @Override public void onVote(MessageContext context, Vote vote) {
        invocations.incrementAndGet();
      }

      @Override public String getGroupId() {
        return null;
      }
    });
    callAll(adapter);
    assertEquals(1, invocations.get());
  }

  @Test
  public void testOutcome() {
    final AtomicInteger invocations = new AtomicInteger();
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new OutcomeFactor() {
      @Override public void onOutcome(MessageContext context, Outcome outcome) {
        invocations.incrementAndGet();
      }

      @Override public String getGroupId() {
        return null;
      }
    });
    callAll(adapter);
    assertEquals(1, invocations.get());
  }
  
  @Test(expected=UnsupportedOperationException.class)
  public void testUnsupported() {
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new NullGroupFactor() {});
    adapter.onMessage(null, new UnknownMessage(0L));
  }
  
  @Test(expected=NullPointerException.class)
  public void testNull() {
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new NullGroupFactor() {});
    adapter.onMessage(null, new Message(0, 0) {
      @Override public MessageType getMessageType() {
        return null;
      }
    });
  }
  
  private static void callAll(MessageHandlerAdapter adapter) {
    adapter.onMessage(null, newProposal());
    adapter.onMessage(null, newVote());
    adapter.onMessage(null, newOutcome());
  }
  
  private static Proposal newProposal() {
    return new Proposal(0L, new String[0], null, 0);
  }
  
  private static Vote newVote() {
    return new Vote(0L, new Response("c", Intent.ACCEPT, null));
  }
  
  private static Outcome newOutcome() {
    return new Outcome(0L, Verdict.COMMIT, null, new Response[0]);
  }
}
