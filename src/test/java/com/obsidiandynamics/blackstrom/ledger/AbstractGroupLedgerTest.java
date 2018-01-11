package com.obsidiandynamics.blackstrom.ledger;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public abstract class AbstractGroupLedgerTest implements TestSupport {
  private static final int MAX_WAIT = 10_000;
  
  private static final String[] TEST_COHORTS = new String[] {"a", "b"};
  
  private static class TestHandler implements MessageHandler {
    private final String groupId;
    private final List<Message> received = new CopyOnWriteArrayList<>();
    
    private volatile long lastBallotId = -1;
    private volatile AssertionError error;

    TestHandler(String groupId) {
      this.groupId = groupId;
    }
    
    @Override
    public void onMessage(MessageContext context, Message message) {
      if (LOG) LOG_STREAM.format("Received %s\n", message);
      final long ballotId = (long) message.getBallotId();
      if (ballotId > lastBallotId) {
        lastBallotId = ballotId;
      } else {
        error = new AssertionError("Last ballot " + lastBallotId + ", got " + ballotId);
        throw error;
      }
      received.add(message);
      context.confirm(message.getMessageId());
    }

    @Override
    public String getGroupId() {
      return groupId;
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
      final TestHandler handler = new TestHandler("test-group");
      handlers.add(handler);
      ledger.attach(handler);
    }
    ledger.init();
    
    for (int i = 0; i < numMessages; i++) {
      appendMessage("test");
    }
    
    Timesert.wait(MAX_WAIT).until(() -> {
      int totalReceived = 0;
      for (TestHandler handler : handlers) {
        assertNull(handler.error);
        totalReceived += handler.received.size();
      }
      assertEquals(numMessages, totalReceived);
    });
    
    ledger.dispose();
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
