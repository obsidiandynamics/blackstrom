package com.obsidiandynamics.blackstrom.initiator;

import static junit.framework.TestCase.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.handler.*;
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
    final Ledger ledger = new SingleNodeQueueLedger();
    final AsyncInitiator initiator = new AsyncInitiator();
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(initiator)
        .build();
    
    final AtomicInteger called = new AtomicInteger();
    ledger.attach((NullGroupMessageHandler) (c, m) -> {
      if (m.getMessageType() == MessageType.NOMINATION) {
        called.incrementAndGet();
        try {
          c.getLedger().append(new Outcome(m.getBallotId(), Verdict.COMMIT, null, new Response[0]));
          
          // second append should do nothing
          c.getLedger().append(new Outcome(m.getBallotId(), Verdict.ABORT, AbortReason.REJECT, new Response[0]));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    
    final CompletableFuture<Outcome> f = initiator.initiate(0, new String[0], null, 0);
    final Outcome outcome = f.get();
    assertNotNull(outcome);
    assertEquals(Verdict.COMMIT, outcome.getVerdict());
    assertEquals(1, called.get());
  }
}
