package com.obsidiandynamics.blackstrom.handler;

import static junit.framework.TestCase.*;

import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runner.*;
import org.powermock.api.mockito.*;
import org.powermock.core.classloader.annotations.*;
import org.powermock.modules.junit4.*;
import org.powermock.reflect.*;

import com.obsidiandynamics.blackstrom.model.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(MessageType.class)
public final class MessageHandlerAdapterTest {
  private static MessageType UNKNOWN;
  
  @BeforeClass
  public static void beforeClass() {
    try {
      UNKNOWN = PowerMockito.mock(MessageType.class);
      Whitebox.setInternalState(UNKNOWN, "name", "UNKNOWN");
      Whitebox.setInternalState(UNKNOWN, "ordinal", MessageType.values().length);
      final MessageType[] messageTypes = new MessageType[MessageType.values().length + 1];
      for (int i = 0; i < MessageType.values().length; i++) {
        messageTypes[i] = MessageType.values()[i];
      }
      messageTypes[messageTypes.length - 1] = UNKNOWN;
      PowerMockito.mockStatic(MessageType.class);
      PowerMockito.when(MessageType.values()).thenReturn(messageTypes);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
  
  @Test
  public void testNomination() {
    final AtomicInteger invocations = new AtomicInteger();
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new NominationHandler() {
      @Override public void onNomination(MessageContext context, Nomination nomination) {
        invocations.incrementAndGet();
      }
    });
    callAll(adapter);
    assertEquals(1, invocations.get());
  }

  @Test
  public void testVote() {
    final AtomicInteger invocations = new AtomicInteger();
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new VoteHandler() {
      @Override public void onVote(MessageContext context, Vote vote) {
        invocations.incrementAndGet();
      }
    });
    callAll(adapter);
    assertEquals(1, invocations.get());
  }

  @Test
  public void testOutcome() {
    final AtomicInteger invocations = new AtomicInteger();
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new OutcomeHandler() {
      @Override public void onOutcome(MessageContext context, Outcome outcome) {
        invocations.incrementAndGet();
      }
    });
    callAll(adapter);
    assertEquals(1, invocations.get());
  }
  
  @Test(expected=UnsupportedOperationException.class)
  public void testUnsupported() {
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new Object());
    adapter.onMessage(null, new Message(null) {
      @Override public MessageType getMessageType() {
        return UNKNOWN;
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
    return new Outcome(null, null, null);
  }
}
