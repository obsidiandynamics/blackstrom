package com.obsidiandynamics.blackstrom.ledger;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.junit.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.bank.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.testmark.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractLedgerTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static final String[] testCohorts = new String[] {"a", "b"};
  private static final Object testObjectives = BankSettlement.forTwo(1_000);

  private final int scale = Testmark.getOptions(Scale.class, Scale.unity()).magnitude();
  
  private class TestHandler implements MessageHandler, Groupable.NullGroup {
    private final List<Message> received = new CopyOnWriteArrayList<>();
    
    private volatile long lastBallotId = -1;
    private volatile AssertionError error;

    @Override
    public void onMessage(MessageContext context, Message message) {
      if (! sandbox.contains(message)) return;
      
      zlg.t("Received %s", z -> z.arg(message));
      final long xid = Long.parseLong(message.getBallotId());
      if (lastBallotId == -1) {
        lastBallotId = xid;
      } else {
        final long expectedBallotId = lastBallotId + 1;
        if (xid != expectedBallotId) {
          error = new AssertionError("Expected ballot " + expectedBallotId + ", got " + xid);
          throw error;
        } else {
          lastBallotId = xid;
        }
      }
      received.add(message);
      context.beginAndConfirm(message);
    }
  }
  
  protected Ledger ledger;
  
  private long messageId;
  
  private final Sandbox sandbox = Sandbox.forInstance(this);
  
  protected final Timesert wait = getWait();
  
  protected abstract Timesert getWait();
  
  protected abstract Ledger createLedger();
  
  @After
  public void afterBase() {
    if (ledger != null) {
      ledger.dispose();
    }
  }
  
  protected void useLedger(Ledger ledger) {
    if (this.ledger != null) this.ledger.dispose();
    this.ledger = ledger;
  }
  
  @Test
  public final void testPubSub() {
    useLedger(createLedger());
    final int numHandlers = 3;
    final int numMessages = 10;
    final List<TestHandler> handlers = new ArrayList<>(numHandlers);
    
    for (int i = 0; i < numHandlers; i++) {
      final TestHandler handler = new TestHandler();
      handlers.add(handler);
      ledger.attach(handler);
    }
    ledger.init();
    
    for (int i = 0; i < numMessages; i++) {
      Threads.sleep(10); // small pauses between publishing allow to test yield-based backoffs for those ledgers that use them
      appendMessage("test", testObjectives);
    }
    
    boolean success = false;
    try {
      wait.until(() -> {
        for (TestHandler handler : handlers) {
          assertNull(handler.error);
          assertEquals(numMessages, handler.received.size());
          long index = 0;
          for (Message m  : handler.received) {
            assertEquals(String.valueOf(index), m.getBallotId());
            index++;
          }
        }
      });
      success = true;
    } finally {
      if (! success) {
        for (TestHandler handler : handlers) {
          System.out.println("---");
          for (Message m : handler.received) {
            System.out.println("- " + m);
          }
        }
      }
    }
    
    ledger.dispose();
  }
  
  @Test
  public final void testOneWay() {
    testOneWay(2, 4, 10_000 * scale);
  }
  
  @Test
  public final void testOneWayBenchmark() {
    Testmark.ifEnabled(() -> {
      testOneWay(1, 1, 2_000_000 * scale);
      testOneWay(1, 2, 2_000_000 * scale);
      testOneWay(1, 4, 2_000_000 * scale);
      testOneWay(2, 4, 1_000_000 * scale);
      testOneWay(2, 8, 1_000_000 * scale);
      testOneWay(4, 8, 500_000 * scale);
      testOneWay(4, 16, 500_000 * scale);
    });
  }
  
  private final void testOneWay(int producers, int consumers, int messagesPerProducer) {
    useLedger(createLedger());
    
    final int backlogTarget = 10_000;
    final int checkInterval = backlogTarget;
    final AtomicLong[] receivedArray = new AtomicLong[consumers];
    for (int i = 0; i < consumers; i++) {
      final AtomicLong received = new AtomicLong();
      receivedArray[i] = received;
      ledger.attach((NullGroupMessageHandler) (c, m) -> {
        if (sandbox.contains(m)) {
          received.incrementAndGet();
        }
      });
    }
    ledger.init();
    
    final LongSupplier totalReceived = () -> {
      long total = 0;
      for (AtomicLong received : receivedArray) {
        total += received.get();
      }
      return total;
    };
    
    final LongSupplier smallestReceived = () -> {
      long smallest = Long.MAX_VALUE;
      for (AtomicLong received : receivedArray) {
        final long r = received.get();
        if (r < smallest) {
          smallest = r;
        }
      }
      return smallest;
    };

    final AtomicLong totalSent = new AtomicLong();
    final long tookMillis = Threads.tookMillis(() -> {
      Parallel.blocking(producers, threadNo -> {
        for (int i = 0; i < messagesPerProducer; i++) {
          appendMessage("test", testObjectives);
          
          if (i != 0 && i % checkInterval == 0) {
            final long sent = totalSent.addAndGet(checkInterval);
            while (sent - smallestReceived.getAsLong() >= backlogTarget) {
              Threads.sleep(1);
            }
          }
        }
      }).run();

      wait.until(() -> {
        assertEquals(producers * messagesPerProducer * (long) consumers, totalReceived.getAsLong());
      });
    });
                                     
    final long totalMessages = (long) producers * messagesPerProducer * consumers;
    System.out.format("One-way: %d/%d prd/cns, %,d msgs took %,d ms, %,.0f msg/s\n", 
                      producers, consumers, totalMessages, tookMillis, (double) totalMessages / tookMillis * 1000);
  }
  
  @Test
  public final void testTwoWay() {
    testTwoWay(10_000 * scale);
  }
  
  @Test
  public final void testTwoWayBenchmark() {
    Testmark.ifEnabled(() -> testTwoWay(2_000_000 * scale));
  }
  
  private final void testTwoWay(int numMessages) {
    useLedger(createLedger());

    final AtomicLong totalSent = new AtomicLong();
    final int backlogTarget = 10_000;
    final int checkInterval = backlogTarget;
    final AtomicLong received = new AtomicLong();
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (sandbox.contains(m) && m.getSource().equals("source")) {
        c.getLedger().append(new Proposal(m.getBallotId(), 0, testCohorts, null, 0).withSource("echo"));
        c.beginAndConfirm(m);
      }
    });
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (m.getSource().equals("echo")) {
        received.incrementAndGet();
        c.beginAndConfirm(m);
      }
    });
    ledger.init();
    
    final long tookMillis = Threads.tookMillis(() -> {
      for (int i = 0; i < numMessages; i++) {
        appendMessage("source", testObjectives);
        
        if (i != 0 && i % checkInterval == 0) {
          final long sent = totalSent.addAndGet(checkInterval);
          while (sent - received.get() > backlogTarget) {
            Threads.sleep(1);
          }
        }
      }
      wait.until(() -> {
        assertEquals(numMessages, received.get());
      });
    });
                                     
    System.out.format("Two-way: %,d took %,d ms, %,d msg/s\n", numMessages, tookMillis, numMessages / tookMillis * 1000);
  }
  
  private void appendMessage(String source, Object objective) {
    ledger.append(new Proposal(String.valueOf(messageId++), 0, testCohorts, objective, 0)
                  .withSource(source)
                  .withShardKey(sandbox.key()));
  }
}
