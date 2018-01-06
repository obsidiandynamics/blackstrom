package com.obsidiandynamics.blackstrom.factor;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class FailureProneFactorTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private static final int MAX_WAIT = 10_000;
  
  private static class TestCohort implements Cohort, Groupable.NullGroup {
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
  
  private static class VoteCollector implements Factor, VoteProcessor, Groupable.NullGroup {
    private final List<Vote> votes = new CopyOnWriteArrayList<>();
    
    @Override
    public void onVote(MessageContext context, Vote vote) {
      votes.add(vote);
    }
  }
  
  private VotingMachine machine;
  
  @After
  public void after() {
    if (machine != null) machine.dispose();
  }
  
  @Test
  public void testInitDisposeProxy() {
    final Ledger ledger = mock(Ledger.class);
    final Cohort c = mock(Cohort.class);
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(c)
        .build();
    
    verify(c).init(notNull());
    machine.dispose();
    verify(c).dispose();
  }
  
  @Test
  public void testNoFault() throws Exception {
    final Ledger ledger = new MultiNodeQueueLedger();
    final VoteCollector v = new VoteCollector();
    final TestCohort c = new TestCohort();
    final Factor fc = new FailureProneFactor(c);
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(fc, v)
        .build();
    
    ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
    
    Timesert.wait(MAX_WAIT).until(() -> {
      assertEquals(1, c.nominations.size());
      assertEquals(1, v.votes.size());
    });
  }
  
  @Test
  public void testRxTxZeroProbability() throws Exception {
    final Ledger ledger = new MultiNodeQueueLedger();
    final TestCohort c = new TestCohort();
    final Factor fc = new FailureProneFactor(c)
        .withRxFailureMode(new DuplicateDelivery(0))
        .withTxFailureMode(new DuplicateDelivery(0));
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
    
    Timesert.wait(MAX_WAIT).until(() -> {
      assertEquals(1, c.nominations.size());
    });
  }
  
  @Test
  public void testRxDuplicate() throws Exception {
    final Ledger ledger = new MultiNodeQueueLedger();
    final TestCohort c = new TestCohort();
    final Factor fc = new FailureProneFactor(c)
        .withRxFailureMode(new DuplicateDelivery(1));
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
    
    Timesert.wait(MAX_WAIT).until(() -> {
      assertEquals(2, c.nominations.size());
    });
  }
  
  @Test
  public void testRxDelayed() throws Exception {
    final Ledger ledger = new MultiNodeQueueLedger();
    final TestCohort c = new TestCohort();
    final int delay = 10;
    final Factor fc = new FailureProneFactor(c)
        .withRxFailureMode(new DelayedDelivery(1, delay));
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    final long took = TestSupport.tookThrowing(() -> {
      ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
      Timesert.wait(MAX_WAIT).until(() -> {
        assertEquals(1, c.nominations.size());
      });
    });
    assertTrue("took=" + took, took >= delay);
  }
  
  @Test
  public void testRxDelayedDuplicate() throws Exception {
    final Ledger ledger = new MultiNodeQueueLedger();
    final TestCohort c = new TestCohort();
    final int delay = 10;
    final Factor fc = new FailureProneFactor(c)
        .withRxFailureMode(new DelayedDuplicateDelivery(1, delay));
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    final long took = TestSupport.tookThrowing(() -> {
      ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
      Timesert.wait(MAX_WAIT).until(() -> {
        assertEquals(2, c.nominations.size());
      });
    });
    assertTrue("took=" + took, took >= delay);
  }
  
  @Test
  public void testTxDuplicate() throws Exception {
    final Ledger ledger = new MultiNodeQueueLedger();
    final VoteCollector v = new VoteCollector();
    final TestCohort c = new TestCohort();
    final Factor fc = new FailureProneFactor(c)
        .withTxFailureMode(new DuplicateDelivery(1));
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(fc, v)
        .build();
    
    ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
    
    Timesert.wait(MAX_WAIT).until(() -> {
      assertEquals(2, v.votes.size());
    });
  }
  
  @Test
  public void testTxDelayed() throws Exception {
    final Ledger ledger = new MultiNodeQueueLedger();
    final VoteCollector v = new VoteCollector();
    final TestCohort c = new TestCohort();
    final int delay = 10;
    final Factor fc = new FailureProneFactor(c)
        .withTxFailureMode(new DelayedDelivery(1, delay));
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(fc, v)
        .build();
    
    final long took = TestSupport.tookThrowing(() -> {
      ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
      Timesert.wait(MAX_WAIT).until(() -> {
        assertEquals(1, v.votes.size());
      });
    });
    assertTrue("took=" + took, took >= delay);
  }
  
  @Test
  public void testTxDelayedDuplicate() throws Exception {
    final Ledger ledger = new MultiNodeQueueLedger();
    final VoteCollector v = new VoteCollector();
    final TestCohort c = new TestCohort();
    final int delay = 10;
    final Factor fc = new FailureProneFactor(c)
        .withTxFailureMode(new DelayedDuplicateDelivery(1, delay));
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(fc, v)
        .build();
    
    final long took = TestSupport.tookThrowing(() -> {
      ledger.append(new Nomination(UUID.randomUUID(), new String[] {"test"}, null, 1000));
      Timesert.wait(MAX_WAIT).until(() -> {
        assertEquals(2, v.votes.size());
      });
    });
    assertTrue("took=" + took, took >= delay);
  }
}
