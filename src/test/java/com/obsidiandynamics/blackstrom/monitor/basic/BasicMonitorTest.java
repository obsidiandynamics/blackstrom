package com.obsidiandynamics.blackstrom.monitor.basic;

import static junit.framework.TestCase.*;

import java.util.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public final class BasicMonitorTest {
  private static final int MAX_WAIT = 60_000;
  
  private BasicMonitor monitor;
  
  private VotingContext context;
  
  private Ledger ledger;
  
  private List<Decision> decisions;
  
  private long messageId;
  
  @Before
  public void before() {
    setMonitor(new BasicMonitor());
    setLedger(new SingleQueueLedger());
    decisions = new ArrayList<>();
    ledger.attach((c, m) -> decisions.add((Decision) m));
    ledger.init(null);
    context = new DefaultVotingContext(ledger);
  }
  
  @After
  public void after() {
    monitor.dispose();
    ledger.dispose();
  }
  
  private void setMonitor(BasicMonitor monitor) {
    if (this.monitor != null) this.monitor.dispose();
    this.monitor = monitor;
  }
  
  private void setLedger(Ledger ledger) {
    if (this.ledger != null) this.ledger.dispose();
    this.ledger = ledger;
  }
  
  @Test
  public void testNominationDecision_oneCohort() {
    UUID ballotId;
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Plea.ACCEPT);
    
    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.COMMIT, decisions.get(0).getOutcome());
    assertEquals(1, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals("ACCEPT", getResponseForCohort(decisions.get(0), "a").getMetadata());
    decisions.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Plea.REJECT);

    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.ABORT, decisions.get(0).getOutcome());
    assertEquals(1, decisions.get(0).getResponses().length);
    assertEquals(Plea.REJECT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals("REJECT", getResponseForCohort(decisions.get(0), "a").getMetadata());
    decisions.clear();
  }
  
  @Test
  public void testNominationDecision_twoCohorts() {
    UUID ballotId;
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Plea.ACCEPT);
    assertEquals(0, decisions.size());
    vote(ballotId, "b", Plea.ACCEPT);
    
    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.COMMIT, decisions.get(0).getOutcome());
    assertEquals(2, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "b").getPlea());
    decisions.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Plea.ACCEPT);
    assertEquals(0, decisions.size());
    vote(ballotId, "b", Plea.REJECT);

    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.ABORT, decisions.get(0).getOutcome());
    assertEquals(2, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals(Plea.REJECT, getResponseForCohort(decisions.get(0), "b").getPlea());
    decisions.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Plea.REJECT);

    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    vote(ballotId, "b", Plea.ACCEPT);
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.ABORT, decisions.get(0).getOutcome());
    assertEquals(1, decisions.get(0).getResponses().length);
    assertEquals(Plea.REJECT, getResponseForCohort(decisions.get(0), "a").getPlea());
    decisions.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Plea.REJECT);

    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    vote(ballotId, "b", Plea.REJECT);
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.ABORT, decisions.get(0).getOutcome());
    assertEquals(1, decisions.get(0).getResponses().length);
    assertEquals(Plea.REJECT, getResponseForCohort(decisions.get(0), "a").getPlea());
    decisions.clear();
  }
  
  @Test
  public void testDuplicateNomination_twoCohorts() {
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    nominate(ballotId, "a", "b", "c");
    vote(ballotId, "a", Plea.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, decisions.size());
    nominate(ballotId, "a", "b", "c");

    TestSupport.sleep(10);
    assertEquals(0, decisions.size());
    vote(ballotId, "b", Plea.ACCEPT);

    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.COMMIT, decisions.get(0).getOutcome());
    assertEquals(2, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "b").getPlea());
    decisions.clear();
    nominate(ballotId, "a", "b", "c");
    assertEquals(0, decisions.size());
  }
  
  @Test
  public void testDuplicateVote_twoCohorts() {
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Plea.ACCEPT);
    vote(ballotId, "a", Plea.REJECT);

    TestSupport.sleep(10);
    assertEquals(0, decisions.size());
    vote(ballotId, "b", Plea.ACCEPT);
    vote(ballotId, "b", Plea.TIMEOUT);

    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.COMMIT, decisions.get(0).getOutcome());
    assertEquals(2, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "b").getPlea());
    decisions.clear();
    vote(ballotId, "b", Plea.REJECT);
    assertEquals(0, decisions.size());
  }
  
  @Test
  public void testVoteWithoutBallot() {
    final UUID ballotId = UUID.randomUUID();
    vote(ballotId, "a", Plea.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, decisions.size());
  }
  
  @Test
  public void testGCNoReap() {
    setMonitor(new BasicMonitor(new BasicMonitorOptions().withDecisionLifetimeMillis(60_000).withGCIntervalMillis(1)));
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Plea.ACCEPT);
    
    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    
    TestSupport.sleep(10);
    assertEquals(1, monitor.getDecisions().size());
  }
  
  @Test
  public void testGCReap() {
    setMonitor(new BasicMonitor(new BasicMonitorOptions().withDecisionLifetimeMillis(1).withGCIntervalMillis(1)));
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Plea.ACCEPT);
    
    Timesert.wait(MAX_WAIT).untilTrue(() -> decisions.size() == 1);
    assertEquals(1, decisions.size());
    
    Timesert.wait(MAX_WAIT).untilTrue(() -> monitor.getDecisions().isEmpty());
  }
  
  private static class TestLedgerException extends Exception {
    private static final long serialVersionUID = 1L;
  }
  
  @Test
  public void testAppendError() throws Exception {
    setLedger(Mockito.mock(Ledger.class));
    Mockito.doThrow(TestLedgerException.class).when(ledger).append(Mockito.any());
    decisions = new ArrayList<>();
    ledger.attach((c, m) -> decisions.add((Decision) m));
    ledger.init(null);
    context = new DefaultVotingContext(ledger);
    
    final UUID ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Plea.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, decisions.size());
  }
  
  private Response getResponseForCohort(Decision decision, String cohort) {
    for (Response response : decision.getResponses()) {
      if (response.getCohort().equals(cohort)) {
        return response;
      }
    }
    return null;
  }

  private void nominate(UUID ballotId, String... cohorts) {
    monitor.onNomination(context, new Nomination(messageId++, ballotId, "Test", cohorts, null, 0));
  }

  private void vote(UUID ballotId, String cohort, Plea plea) {
    monitor.onVote(context, new Vote(messageId++, ballotId, "Test", new Response(cohort, plea, plea.name())));
  }
}
