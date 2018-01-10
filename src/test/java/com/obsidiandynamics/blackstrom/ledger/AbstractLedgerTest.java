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

public abstract class AbstractLedgerTest implements TestSupport {
  private static final int MAX_WAIT = 10_000;
  
  private static final String[] TEST_COHORTS = new String[] {"a", "b"};
  
  private static class TestHandler implements MessageHandler, Groupable.NullGroup {
    private final List<Message> received = new CopyOnWriteArrayList<>();
    
    private volatile long lastBallotId = -1;
    private volatile AssertionError error;

    @Override
    public void onMessage(MessageContext context, Message message) {
      if (LOG) LOG_STREAM.format("Received %s\n", message);
      final long ballotId = (long) message.getBallotId();
      if (lastBallotId == -1) {
        lastBallotId = ballotId;
      } else {
        final long expectedBallotId = lastBallotId + 1;
        if (ballotId != expectedBallotId) {
          error = new AssertionError("Expected ballot " + expectedBallotId + ", got " + ballotId);
          throw error;
        } else {
          lastBallotId = ballotId;
        }
      }
      received.add(message);
      context.getLedger().confirm(getGroupId(), message.getMessageId());
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
    ledger.init();
    
    for (int i = 0; i < numMessages; i++) {
      appendMessage("test");
    }
    
    Timesert.wait(MAX_WAIT).until(() -> {
      for (TestHandler handler : handlers) {
        assertNull(handler.error);
        assertEquals(numMessages, handler.received.size());
        long index = 0;
        for (Message m  : handler.received) {
          assertEquals(index, m.getBallotId());
          index++;
        }
      }
    });
    
    ledger.dispose();
  }
  
  @Test
  public final void testOneWayBenchmark() {
    ledger = createLedger();
    final int numMessages = 10_000;
    
    final AtomicLong received = new AtomicLong();
    ledger.attach((NullGroupMessageHandler) (c, m) -> received.incrementAndGet());
    ledger.init();
    
    final long took = TestSupport.took(() -> {
      for (int i = 0; i < numMessages; i++) {
        appendMessage("test");
      }
      Timesert.wait(MAX_WAIT).until(() -> {
        assertEquals(numMessages, received.get());
      });
    });
                                     
    System.out.format("One-way: %,d took %,d ms, %,.0f msgs/sec\n", 
                      numMessages, took, (float) numMessages / took * 1000);
  }
  
  @Test
  public final void testTwoWayBenchmark() {
    ledger = createLedger();
    final int numMessages = 10_000;
    
    final AtomicLong received = new AtomicLong();
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (m.getSource().equals("source")) {
        try {
          c.getLedger().append(new Nomination(m.getMessageId(), 0, TEST_COHORTS, null, 0).withSource("echo"));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        c.getLedger().confirm(null, m.getMessageId());
      }
    });
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (m.getSource().equals("echo")) {
        received.incrementAndGet();
        c.getLedger().confirm(null, m.getMessageId());
      }
    });
    ledger.init();
    
    final long took = TestSupport.took(() -> {
      for (int i = 0; i < numMessages; i++) {
        appendMessage("source");
      }
      Timesert.wait(MAX_WAIT).until(() -> {
        assertEquals(numMessages, received.get());
      });
    });
                                     
    System.out.format("Two-way: %,d took %,d ms, %,d msgs/sec\n", numMessages, took, numMessages / took * 1000);
  }
  
  private void appendMessage(String source) {
    try {
      ledger.append(new Nomination(messageId++, 0, TEST_COHORTS, null, 0).withSource(source));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private Ledger createLedger() {
    return createLedgerImpl();
  }
  
  protected abstract Ledger createLedgerImpl();
}
