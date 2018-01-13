package com.obsidiandynamics.blackstrom.monitor;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.mockito.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class DefaultMonitorTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final Timesert wait = Wait.SHORT;
  
  private final InitContext initContext = new DefaultInitContext(new Ledger() {
    @Override public void attach(MessageHandler handler) {
      ledger.attach(handler);
    }

    @Override public void append(Message message) throws Exception {
      ledger.append(message);
    }
    
    @Override public void confirm(Object handlerId, Object messageId) {
      ledger.confirm(handlerId, messageId);
    }
    
    @Override public void init() {
      ledger.init();
    }
    
    @Override public void dispose() {
      ledger.dispose();
    }
  });
  
  private DefaultMonitor monitor;
  
  private MessageContext context;
  
  private Ledger ledger;
  
  private final List<Vote> votes = new CopyOnWriteArrayList<>();
  
  private final List<Outcome> outcomes = new CopyOnWriteArrayList<>();
  
  @Before
  public void before() {
    setMonitorAndInit(new DefaultMonitor());
    setLedger(new SingleNodeQueueLedger());
    ledger.attach((NullGroupMessageHandler) (c, m) -> { 
      if (m.getMessageType() == MessageType.OUTCOME) {
        outcomes.add((Outcome) m);
      } else {
        votes.add((Vote) m);
      }
    });
    ledger.init();
    context = new DefaultMessageContext(ledger, null);
  }
  
  @After
  public void after() {
    monitor.dispose();
    ledger.dispose();
  }
  
  private void setMonitorAndInit(DefaultMonitor monitor) {
    if (this.monitor != null) this.monitor.dispose();
    this.monitor = monitor;
    monitor.init(initContext);
  }
  
  private void setLedger(Ledger ledger) {
    if (this.ledger != null) this.ledger.dispose();
    this.ledger = ledger;
  }
  
  @Test
  public void testInitialisation() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withGroupId("test-monitor")));
    assertEquals("test-monitor", monitor.getGroupId());
  }
  
  @Test
  public void testNominationOutcome_oneCohort() {
    UUID ballotId;
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Pledge.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    assertEquals("ACCEPT", getResponseForCohort(outcomes.get(0), "a").getMetadata());
    outcomes.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Pledge.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.REJECT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    assertEquals("REJECT", getResponseForCohort(outcomes.get(0), "a").getMetadata());
    outcomes.clear();
  }
  
  @Test
  public void testNominationOutcome_twoCohorts() {
    UUID ballotId;
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Pledge.ACCEPT);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Pledge.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getPledge());
    outcomes.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Pledge.ACCEPT);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Pledge.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    assertEquals(Pledge.REJECT, getResponseForCohort(outcomes.get(0), "b").getPledge());
    outcomes.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Pledge.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    vote(ballotId, "b", Pledge.ACCEPT);
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.REJECT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    outcomes.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Pledge.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    vote(ballotId, "b", Pledge.REJECT);
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.REJECT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    outcomes.clear();
  }
  
  @Test
  public void testDuplicateNomination_twoCohorts() {
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    nominate(ballotId, "a", "b", "c");
    vote(ballotId, "a", Pledge.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    nominate(ballotId, "a", "b", "c");

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Pledge.ACCEPT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getPledge());
    outcomes.clear();
    nominate(ballotId, "a", "b", "c");
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testDuplicateVote_twoCohorts() {
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Pledge.ACCEPT);
    vote(ballotId, "a", Pledge.REJECT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Pledge.ACCEPT);
    vote(ballotId, "b", Pledge.TIMEOUT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getPledge());
    outcomes.clear();
    vote(ballotId, "b", Pledge.REJECT);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testVoteWithoutBallot() {
    final UUID ballotId = UUID.randomUUID();
    vote(ballotId, "a", Pledge.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testGCNoReap() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions()
                                       .withOutcomeLifetime(60_000)
                                       .withGCInterval(1)));
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Pledge.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    
    TestSupport.sleep(10);
    assertEquals(1, monitor.getOutcomes().size());
  }
  
  @Test
  public void testGCReap() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withOutcomeLifetime(1).withGCInterval(1)));
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Pledge.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    
    wait.untilTrue(() -> monitor.getOutcomes().isEmpty());
  }
  
  private static class TestLedgerException extends Exception {
    private static final long serialVersionUID = 1L;
  }
  
  @Test
  public void testAppendError() throws Exception {
    setLedger(Mockito.mock(Ledger.class));
    Mockito.doThrow(TestLedgerException.class).when(ledger).append(Mockito.any());
    ledger.attach((NullGroupMessageHandler) (c, m) -> outcomes.add((Outcome) m));
    ledger.init();
    context = new DefaultMessageContext(ledger, null);
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Pledge.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testTimeout_twoCohorts() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(1)));
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, 1, "a", "b");
    vote(ballotId, "a", Pledge.ACCEPT);
    
    wait.until(numVotesIsAtLeast(1));
    assertEquals(0, outcomes.size());
    assertEquals(ballotId, votes.get(0).getBallotId());
    assertEquals(Pledge.TIMEOUT, votes.get(0).getResponse().getPledge());
    
    // feed the timeout back into the monitor - should produce a rejection
    vote(ballotId, "b", Pledge.TIMEOUT);
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.ABORT, outcomes.get(0).getVerdict());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    assertEquals(Pledge.TIMEOUT, getResponseForCohort(outcomes.get(0), "b").getPledge());
    
    // subsequent votes should have no effect
    vote(ballotId, "b", Pledge.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(1, outcomes.size());
  }
  
  @Test
  public void testNoTimeout_twoCohorts() {
    setMonitorAndInit(new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(1)));
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, 10_000, "a", "b");
    vote(ballotId, "a", Pledge.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    assertEquals(0, votes.size());
    
    vote(ballotId, "b", Pledge.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Verdict.COMMIT, outcomes.get(0).getVerdict());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getPledge());
    assertEquals(Pledge.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getPledge());
  }
  
  private Runnable numVotesIsAtLeast(int size) {
    return () -> assertTrue("votes.size=" + votes.size(), votes.size() >= size);
  }
  
  private Runnable numOutcomesIs(int size) {
    return () -> assertEquals(size, outcomes.size());
  }
  
  private Response getResponseForCohort(Outcome outcome, String cohort) {
    for (Response response : outcome.getResponses()) {
      if (response.getCohort().equals(cohort)) {
        return response;
      }
    }
    return null;
  }

  private void nominate(UUID ballotId, String... cohorts) {
    nominate(ballotId, Integer.MAX_VALUE, cohorts);
  }

  private void nominate(UUID ballotId, int timeout, String... cohorts) {
    monitor.onNomination(context, new Nomination(ballotId, cohorts, null, timeout));
  }

  private void vote(UUID ballotId, String cohort, Pledge pledge) {
    monitor.onVote(context, new Vote(ballotId, new Response(cohort, pledge, pledge.name())));
  }
}
