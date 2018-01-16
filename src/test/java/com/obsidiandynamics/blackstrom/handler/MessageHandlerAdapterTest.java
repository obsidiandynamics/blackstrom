package com.obsidiandynamics.blackstrom.handler;

import static junit.framework.TestCase.*;

import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class MessageHandlerAdapterTest {
  interface NominationFactor extends Factor, NominationProcessor, Groupable.NullGroup {};
  
  interface VoteFactor extends Factor, VoteProcessor, Groupable.NullGroup {};
  
  interface OutcomeFactor extends Factor, OutcomeProcessor, Groupable.NullGroup {};
  
  @Test
  public void testNominationAndGroup() {
    final AtomicInteger invocations = new AtomicInteger();
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new NominationFactor() {
      @Override public void onNomination(MessageContext context, Nomination nomination) {
        invocations.incrementAndGet();
      }
      
      @Override public String getGroupId() {
        return "test-nomination";
      }
    });
    callAll(adapter);
    assertEquals(1, invocations.get());
    assertEquals("test-nomination", adapter.getGroupId());
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
    adapter.onMessage(null, new UnknownMessage(null));
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
    adapter.onMessage(null, newNomination());
    adapter.onMessage(null, newVote());
    adapter.onMessage(null, newOutcome());
  }
  
  private static Nomination newNomination() {
    return new Nomination(null, null, null, 0);
  }
  
  private static Vote newVote() {
    return new Vote(null, null);
  }
  
  private static Outcome newOutcome() {
    return new Outcome(null, null, null, null);
  }
}
