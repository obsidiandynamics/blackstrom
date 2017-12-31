package com.obsidiandynamics.blackstrom.factor;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class FailureProneFactorTest {
  private static class TestCohort implements Cohort {
    private final List<Nomination> nominations = new CopyOnWriteArrayList<>();
    
    @Override
    public void onNomination(MessageContext context, Nomination nomination) {
      nominations.add(nomination);
      try {
        context.vote(nomination.getBallotId(), "test", Pledge.ACCEPT, null);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onOutcome(MessageContext context, Outcome outcome) {
      throw new UnsupportedOperationException();
    }
  }
  
  private static final int MAX_WAIT = 10_000;
  
  private VotingMachine machine;
  
  @After
  public void after() {
    if (machine != null) machine.dispose();
  }
  
//  @Test
//  public void testNoFault() throws Exception {
//    final Ledger ledger = new MultiNodeQueueLedger();
//    final TestCohort c = new TestCohort();
//    final Factor f = new FailureProneFactor(c)
//        .withFailureModes();
//    machine = VotingMachine.builder()
//        .withLedger(ledger)
//        .withFactors(f)
//        .build();
//    ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
//    
//    Timesert.wait(MAX_WAIT).until(() -> {
//      assertEquals(1, c.nominations.size());
//    });
//  }

}
