package com.obsidiandynamics.blackstrom.ledger.multiqueue;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class MultiQueueLedgerTest {
  private static final int MAX_WAIT = 1_000;
  
  private static class TestHandler implements MessageHandler {
    private final List<Message> received = new CopyOnWriteArrayList<>();
    
    private volatile int lastMessageId = -1;
    private volatile AssertionError error;

    @Override
    public void onMessage(VotingContext context, Message message) {
      final int messageId = (Integer) message.getMessageId();
      if (lastMessageId == -1) {
        lastMessageId = messageId;
      } else {
        final int expectedMessageId = lastMessageId + 1;
        if (messageId != expectedMessageId) {
          error = new AssertionError("Expected message " + expectedMessageId + ", got " + messageId);
          throw error;
        } else {
          lastMessageId = messageId;
        }
      }
      received.add(message);
    }
  }
  
  private static class TestMessage extends Message {
    protected TestMessage(Object messageId) {
      super(messageId, "TestBallot", "TestSource");
    }

    @Override
    public MessageType getMessageType() {
      return null;
    }
  }
  
  private MultiQueueLedger ledger;
  
  private final AtomicInteger messageId = new AtomicInteger();
  
  @Before
  public void before() {
    messageId.set(0);
  }
  
  @After
  public void after() {
    if (ledger != null) {
      ledger.dispose();
    }
  }
  
  @Test
  public void testPubSub() {
    ledger = new MultiQueueLedger(10);
    final int numHandlers = 3;
    final int numMessages = 5;
    final List<TestHandler> handlers = new ArrayList<>(numHandlers);
    
    for (int i = 0; i < numHandlers; i++) {
      final TestHandler handler = new TestHandler();
      handlers.add(handler);
      ledger.attach(handler);
    }
    
    for (int i = 0; i < numMessages; i++) {
      appendMessage();
    }
    
    Timesert.wait(MAX_WAIT).until(() -> {
      for (TestHandler handler : handlers) {
        assertNull(handler.error);
        assertEquals(numMessages, handler.received.size());
        int index = 0;
        for (Message m  : handler.received) {
          assertEquals(index, m.getMessageId());
          index++;
        }
      }
    });
    
    ledger.dispose();
  }
  
  private void appendMessage() {
    try {
      ledger.append(new TestMessage(messageId.getAndIncrement()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
