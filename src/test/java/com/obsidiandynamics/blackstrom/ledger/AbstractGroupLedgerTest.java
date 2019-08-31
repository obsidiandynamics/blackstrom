package com.obsidiandynamics.blackstrom.ledger;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;

public abstract class AbstractGroupLedgerTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static final String[] TEST_COHORTS = new String[] {"a", "b"};
  
  private class TestHandler implements MessageHandler {
    private final String groupId;
    private final List<Message> received = new CopyOnWriteArrayList<>();
    
    private volatile long lastBallotId = -1;
    private volatile AssertionError error;

    TestHandler(String groupId) {
      this.groupId = groupId;
    }
    
    @Override
    public void onMessage(MessageContext context, Message message) {
      if (! sandbox.contains(message)) return;
      
      zlg.t("Received %s", z -> z.arg(message));
      assertTrue(context.isAssigned(message));
      final long xid = Long.parseLong(message.getXid());
      if (xid > lastBallotId) {
        lastBallotId = xid;
      } else {
        error = new AssertionError("Last ballot " + lastBallotId + ", got " + xid);
        throw error;
      }
      received.add(message);
      context.beginAndConfirm(message);
    }

    @Override
    public String getGroupId() {
      return groupId;
    }
  }
  
  private Ledger ledger;
  
  private long messageId;
  
  private final Sandbox sandbox = Sandbox.forInstance(this);
  
  private final Timesert wait = getWait();
  
  protected abstract Timesert getWait();
  
  protected abstract Ledger createLedger();
  
  @After
  public void afterBase() {
    if (ledger != null) {
      ledger.dispose();
    }
  }
  
  @Test
  public final void testPubSub() {
    ledger = createLedger();
    ledger.init();
    
    final int numHandlers = 3;
    final int numMessages = 5;
    final List<TestHandler> handlers = new ArrayList<>(numHandlers);
    final List<Object> handlerIds = new ArrayList<>();
    
    // register an initial handler first, allowing the ledger to direct traffic to it following an initial rebalance
    final TestHandler initialHandler = new TestHandler("test-group");
    handlers.add(initialHandler);
    final Object initialHandlerId = ledger.attach(initialHandler);
    assertNotNull(initialHandlerId); // group-based handler ID can never be null
    handlerIds.add(initialHandlerId);
    
    // wait until at least one message is received by the initial handler before adding new ones (avoids spurious
    // rebalancing and duplicate messages depending on ledger implementation, e.g. Kafka)
    final AtomicBoolean awaitedInitialHandler = new AtomicBoolean();
    final Thread addMoreHandlersThread = new Thread(() -> {
      wait.until(() -> {
        assertTrue(handlers.get(0).received.size() > 0);
      });
      
      for (int i = 0; i < numHandlers - 1; i++) {
        final TestHandler handler = new TestHandler("test-group");
        handlers.add(handler);
        final Object handlerId = ledger.attach(handler);
        assertNotNull(handlerId); // group-based handler ID can never be null
        handlerIds.add(handlerId);
      }
      
      awaitedInitialHandler.set(true);
    }, "AddMoreHandlersThread");
    addMoreHandlersThread.start();

    for (int i = 0; i < numMessages; i++) {
      appendMessage("test");
    }
    
    // wait for all handlers to join before proceeding with further assertions
    Threads.deferInterrupt(addMoreHandlersThread::join);
    assertTrue(awaitedInitialHandler.get());
    
    boolean successCountingMessages = false;
    try {
      wait.until(() -> {
        int totalReceived = 0;
        for (TestHandler handler : handlers) {
          assertNull(handler.error);
          totalReceived += handler.received.size();
        }
        assertEquals(numMessages, totalReceived);
      });
      successCountingMessages = true;
    } finally {
      if (! successCountingMessages) {
        for (TestHandler handler : handlers) {
          System.out.println("---");
          for (Message m : handler.received) {
            System.out.println("- " + m);
          }
        }
      }
    }
    
    boolean successCountingAssignees = false;
    try {
      wait.until(() -> {
        int totalAssignees = 0;
        for (Object handlerId : handlerIds) {
          if (ledger.isAssigned(handlerId, 0)) { // shard 0 should work with all ledger types
            totalAssignees += 1;
          }
        }
        assertEquals(1, totalAssignees);
      });
      successCountingAssignees = true;
    } finally {
      if (! successCountingAssignees) {
        for (Object handlerId : handlerIds) {
          System.out.format("handler: %s, isAssigned: %b\n", handlerId, ledger.isAssigned(handlerId, 0));
        }
      }
    }
    
    ledger.dispose();
  }
  
  private void appendMessage(String source) {
    ledger.append(new Proposal(String.valueOf(messageId++), 0, TEST_COHORTS, null, 0)
                  .withSource(source)
                  .withShardKey(sandbox.key()));
  }
}
