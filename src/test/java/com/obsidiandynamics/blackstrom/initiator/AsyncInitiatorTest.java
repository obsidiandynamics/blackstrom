package com.obsidiandynamics.blackstrom.initiator;

import static junit.framework.TestCase.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class AsyncInitiatorTest {
  private Manifold manifold;
  
  @After
  public void after() {
    if (manifold != null) {
      manifold.dispose();
    }
  }
  
  @Test
  public void testFuture() throws Exception {
    final Ledger ledger = new SingleNodeQueueLedger();
    final AsyncInitiator initiator = new AsyncInitiator();
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(initiator)
        .build();
    
    final AtomicInteger called = new AtomicInteger();
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (m.getMessageType() == MessageType.PROPOSAL) {
        called.incrementAndGet();
        try {
          c.getLedger().append(new Outcome(m.getXid(), Resolution.COMMIT, null, new Response[0], null));
          
          // second append should do nothing
          c.getLedger().append(new Outcome(m.getXid(), Resolution.ABORT, AbortReason.REJECT, new Response[0], null));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    
    final CompletableFuture<Outcome> f = initiator.initiate(new Proposal("B0", new String[0], null, 0));
    final Outcome outcome = f.get();
    assertNotNull(outcome);
    assertEquals(Resolution.COMMIT, outcome.getResolution());
    assertEquals(1, called.get());
  }
}
