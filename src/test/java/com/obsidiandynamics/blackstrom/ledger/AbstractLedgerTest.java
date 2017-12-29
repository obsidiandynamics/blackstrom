package com.obsidiandynamics.blackstrom.ledger;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public abstract class AbstractLedgerTest {
  private static final int MAX_WAIT = 60_000;
  
  private static class TestHandler implements MessageHandler {
    private final List<Message> received = new CopyOnWriteArrayList<>();
    
    private volatile long lastMessageId = -1;
    private volatile AssertionError error;

    @Override
    public void onMessage(MessageContext context, Message message) {
      final long messageId = (Long) message.getMessageId();
      if (lastMessageId == -1) {
        lastMessageId = messageId;
      } else {
        final long expectedMessageId = lastMessageId + 1;
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
    protected TestMessage(Object messageId, String source) {
      super(messageId, "TestBallot", source);
    }

    @Override
    public MessageType getMessageType() {
      return null;
    }
  }
  
  private Ledger ledger;
  
  private long messageId;
  
  @After
  public void after() {
    if (ledger != null) {
      ledger.dispose();
    }
  }
  
  @Test
  public final void testPubSub() {
    ledger = createLedger();
    final int numHandlers = 3;
    final int numMessages = 5;
    final List<TestHandler> handlers = new ArrayList<>(numHandlers);
    
    for (int i = 0; i < numHandlers; i++) {
      final TestHandler handler = new TestHandler();
      handlers.add(handler);
      ledger.attach(handler);
    }
    ledger.init(null);
    
    for (int i = 0; i < numMessages; i++) {
      appendMessage("test");
    }
    
    Timesert.wait(MAX_WAIT).until(() -> {
      for (TestHandler handler : handlers) {
        assertNull(handler.error);
        assertEquals(numMessages, handler.received.size());
        long index = 0;
        for (Message m  : handler.received) {
          assertEquals(index, m.getMessageId());
          index++;
        }
      }
    });
    
    ledger.dispose();
  }
  
  @Test
  public final void testOneWayBenchmark() {
    ledger = createLedger();
    final int numMessages = 100_000;
    
    final AtomicLong received = new AtomicLong();
    ledger.attach((c, m) -> received.incrementAndGet());
    ledger.init(null);
    
    final long took = TestSupport.took(() -> {
      for (int i = 0; i < numMessages; i++) {
        appendMessage("test");
      }
      Timesert.wait(MAX_WAIT).untilTrue(() -> received.get() == numMessages);
    });
                                     
    System.out.format("One-way: %,d took %,d ms, %,d msgs/sec\n", numMessages, took, numMessages / took * 1000);
  }
  
  @Test
  public final void testTwoWayBenchmark() {
    ledger = createLedger();
    final int numMessages = 100_000;
    
    final AtomicLong received = new AtomicLong();
    ledger.attach((c, m) -> {
      if (m.getSource() == "source") {
        try {
          c.getLedger().append(new TestMessage(m.getMessageId(), "echo"));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    ledger.attach((c, m) -> {
      if (m.getSource() == "echo") {
        received.incrementAndGet();
      }
    });
    ledger.init(null);
    
    final long took = TestSupport.took(() -> {
      for (int i = 0; i < numMessages; i++) {
        appendMessage("source");
      }
      Timesert.wait(MAX_WAIT).untilTrue(() -> received.get() == numMessages);
    });
                                     
    System.out.format("Two-way: %,d took %,d ms, %,d msgs/sec\n", numMessages, took, numMessages / took * 1000);
  }
  
  private void appendMessage(String source) {
    try {
      ledger.append(new TestMessage(messageId++, source));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private Ledger createLedger() {
    return createLedgerImpl();
  }
  
  protected abstract Ledger createLedgerImpl();
}
