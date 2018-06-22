package com.obsidiandynamics.blackstrom.factor;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.threads.*;

@RunWith(Parameterized.class)
public final class FallibleFactorTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private static class UnknownFailureMode extends FailureMode {
    UnknownFailureMode() {
      super(1);
    }

    @Override
    public FailureType getFailureType() {
      return FailureType.$UNKNOWN;
    }
  }

  
  private static class TestCohort implements Cohort, Groupable.NullGroup {
    private final List<Proposal> proposals = new CopyOnWriteArrayList<>();
    
    @Override
    public void onProposal(MessageContext context, Proposal proposal) {
      proposals.add(proposal);
      try {
        context.getLedger().append(new Vote(proposal.getXid(), new Response("test", Intent.ACCEPT, null)));
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
  
  private Manifold manifold;
  
  private final Timesert wait = Wait.SHORT;
  
  @After
  public void after() {
    if (manifold != null) manifold.dispose();
  }
  
  @Test
  public void testAttachFailure() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final AtomicReference<Exception> causeRef = new AtomicReference<>();
    final Cohort cohort = LambdaCohort.builder()
        .onProposal((c, m) -> {
          try {
            c.getLedger().attach(null);
          } catch (Exception e) {
            causeRef.set(e);
          }
        })
        .onOutcome((c, m) -> {})
        .build();
    final Factor fc = new FallibleFactor(cohort);
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
    wait.until(() -> {
      assertTrue(causeRef.get() instanceof UnsupportedOperationException);
    });
  }
  
  @Test
  public void testInitDisposeProxy() {
    final Ledger ledger = mock(Ledger.class);
    final Cohort c = mock(Cohort.class);
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(c)
        .build();
    
    verify(c).init(notNull());
    manifold.dispose();
    verify(c).dispose();
  }
  
  @Test
  public void testNoFault() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final VoteCollector v = new VoteCollector();
    final TestCohort c = new TestCohort();
    final Factor fc = new FallibleFactor(c);
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc, v)
        .build();
    
    ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
    
    wait.until(() -> {
      assertEquals(1, c.proposals.size());
      assertEquals(1, v.votes.size());
    });
  }
  
  @Test
  public void testRxTxZeroProbability() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final TestCohort c = new TestCohort();
    final Factor fc = new FallibleFactor(c)
        .withRxFailureMode(new DuplicateDelivery(0))
        .withTxFailureMode(new DuplicateDelivery(0));
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
    
    wait.until(() -> {
      assertEquals(1, c.proposals.size());
    });
  }
  
  @Test
  public void testRxDuplicate() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final TestCohort c = new TestCohort();
    final Factor fc = new FallibleFactor(c)
        .withRxFailureMode(new DuplicateDelivery(1));
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
    
    wait.until(() -> {
      assertEquals(2, c.proposals.size());
    });
  }
  
  @Test
  public void testRxDelayed() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final TestCohort c = new TestCohort();
    final int delay = 10;
    final Factor fc = new FallibleFactor(c)
        .withRxFailureMode(new DelayedDelivery(1, delay));
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    final long tookMillis = Threads.tookMillis(() -> {
      ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
      wait.until(() -> {
        assertEquals(1, c.proposals.size());
      });
    });
    assertTrue("tookMillis=" + tookMillis, tookMillis >= delay);
  }
  
  @Test
  public void testRxDelayedDuplicate() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final TestCohort c = new TestCohort();
    final int delay = 10;
    final Factor fc = new FallibleFactor(c)
        .withRxFailureMode(new DelayedDuplicateDelivery(1, delay));
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    final long tookMillis = Threads.tookMillis(() -> {
      ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
      wait.until(() -> {
        assertEquals(2, c.proposals.size());
      });
    });
    assertTrue("tookMillis=" + tookMillis, tookMillis >= delay);
  }
  
  @Test(expected=UnsupportedOperationException.class)
  public void testRxUnknown() {
    final TestCohort c = new TestCohort();
    final FallibleFactor fc = new FallibleFactor(c)
        .withRxFailureMode(new UnknownFailureMode());
    
    fc.onProposal(null, new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
  }
  
  @Test
  public void testTxDuplicate() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final VoteCollector v = new VoteCollector();
    final TestCohort c = new TestCohort();
    final Factor fc = new FallibleFactor(c)
        .withTxFailureMode(new DuplicateDelivery(1));
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc, v)
        .build();
    
    ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
    
    wait.until(() -> {
      assertEquals(2, v.votes.size());
    });
  }
  
  @Test
  public void testTxDelayed() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final VoteCollector v = new VoteCollector();
    final TestCohort c = new TestCohort();
    final int delay = 10;
    final Factor fc = new FallibleFactor(c)
        .withTxFailureMode(new DelayedDelivery(1, delay));
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc, v)
        .build();
    
    final long tookMillis = Threads.tookMillis(() -> {
      ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
      wait.until(() -> {
        assertEquals(1, v.votes.size());
      });
    });
    assertTrue("tookMillis=" + tookMillis, tookMillis >= delay);
  }
  
  @Test
  public void testTxDelayedDuplicate() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final VoteCollector v = new VoteCollector();
    final TestCohort c = new TestCohort();
    final int delay = 10;
    final Factor fc = new FallibleFactor(c)
        .withTxFailureMode(new DelayedDuplicateDelivery(1, delay));
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc, v)
        .build();
    
    final long tookMillis = Threads.tookMillis(() -> {
      ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
      wait.until(() -> {
        assertEquals(2, v.votes.size());
      });
    });
    assertTrue("tookMillis=" + tookMillis, tookMillis >= delay);
  }
  
  @Test
  public void testTxUnknown() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final AtomicReference<Exception> causeRef = new AtomicReference<>();
    final Cohort cohort = LambdaCohort.builder()
        .onProposal((c, m) -> {
          try {
            c.getLedger().append(new Vote(UUID.randomUUID().toString(), new Response("test", Intent.ACCEPT, null)));
          } catch (Exception e) {
            causeRef.set(e);
          }
        })
        .onOutcome((c, m) -> {})
        .build();
    final Factor fc = new FallibleFactor(cohort)
        .withTxFailureMode(new UnknownFailureMode());
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(fc)
        .build();
    
    ledger.append(new Proposal(UUID.randomUUID().toString(), new String[] {"test"}, null, 1000));
    wait.until(() -> {
      assertTrue(causeRef.get() instanceof UnsupportedOperationException);
    });
  }
}
