package com.obsidiandynamics.blackstrom.initiator;

import static junit.framework.TestCase.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class AsyncInitiatorTest {
  private VotingMachine machine;
  
  @After
  public void after() {
    if (machine != null) {
      machine.dispose();
    }
  }
  
  @Test
  public void testFuture() throws Exception {
    final Ledger ledger = new SingleLinkedQueueLedger();
    final AsyncInitiator initiator = new AsyncInitiator("async");
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withInitiator(initiator)
        .build();
    
    final AtomicInteger called = new AtomicInteger();
    ledger.attach((c, m) -> {
      if (m.getMessageType() == MessageType.NOMINATION) {
        called.incrementAndGet();
        try {
          c.getLedger().append(new Decision(m.getMessageId(), m.getBallotId(), "decider", Verdict.COMMIT, new Response[0]));
          
          // second append should do nothing
          c.getLedger().append(new Decision(m.getMessageId(), m.getBallotId(), "decider", Verdict.ABORT, new Response[0]));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    
    final CompletableFuture<Decision> f = initiator.initiate(0, new String[0], null, 0);
    final Decision decision = f.get();
    assertNotNull(decision);
    assertEquals(Verdict.COMMIT, decision.getVerdict());
    assertEquals(1, called.get());
  }
}
