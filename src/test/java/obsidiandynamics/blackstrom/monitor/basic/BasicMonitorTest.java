package obsidiandynamics.blackstrom.monitor.basic;

import static junit.framework.TestCase.*;

import java.util.*;

import org.junit.*;

import obsidiandynamics.blackstrom.handler.*;
import obsidiandynamics.blackstrom.ledger.*;
import obsidiandynamics.blackstrom.ledger.direct.*;
import obsidiandynamics.blackstrom.model.*;

public final class BasicMonitorTest {
  private BasicMonitor monitor;
  
  private VotingContext context;
  
  private Ledger ledger;
  
  private List<Decision> decisions;
  
  private long messageId;
  
  @Before
  public void before() {
    monitor = new BasicMonitor();
    ledger = new DirectLedger();
    decisions = new ArrayList<>();
    ledger.attach((c, m) -> decisions.add((Decision) m));
    context = new DefaultVotingContext(ledger);
  }
  
  @After
  public void after() {
    monitor.dispose();
    ledger.dispose();
  }
  
  @Test
  public void testNominationDecision_oneCohort() {
    UUID ballotId;
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Plea.ACCEPT);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.ACCEPT, decisions.get(0).getOutcome());
    assertEquals(1, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals("ACCEPT", getResponseForCohort(decisions.get(0), "a").getMetadata());
    decisions.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a");
    vote(ballotId, "a", Plea.REJECT);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.REJECT, decisions.get(0).getOutcome());
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
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.ACCEPT, decisions.get(0).getOutcome());
    assertEquals(2, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "b").getPlea());
    decisions.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Plea.ACCEPT);
    assertEquals(0, decisions.size());
    vote(ballotId, "b", Plea.REJECT);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.REJECT, decisions.get(0).getOutcome());
    assertEquals(2, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals(Plea.REJECT, getResponseForCohort(decisions.get(0), "b").getPlea());
    decisions.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Plea.REJECT);
    assertEquals(1, decisions.size());
    vote(ballotId, "b", Plea.ACCEPT);
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.REJECT, decisions.get(0).getOutcome());
    assertEquals(1, decisions.get(0).getResponses().length);
    assertEquals(Plea.REJECT, getResponseForCohort(decisions.get(0), "a").getPlea());
    decisions.clear();
    
    ballotId = UUID.randomUUID();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Plea.REJECT);
    assertEquals(1, decisions.size());
    vote(ballotId, "b", Plea.REJECT);
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.REJECT, decisions.get(0).getOutcome());
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
    assertEquals(0, decisions.size());
    nominate(ballotId, "a", "b", "c");
    assertEquals(0, decisions.size());
    vote(ballotId, "b", Plea.ACCEPT);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.ACCEPT, decisions.get(0).getOutcome());
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
    assertEquals(0, decisions.size());
    vote(ballotId, "b", Plea.ACCEPT);
    vote(ballotId, "b", Plea.REJECT);
    assertEquals(1, decisions.size());
    assertEquals(ballotId, decisions.get(0).getBallotId());
    assertEquals(Outcome.ACCEPT, decisions.get(0).getOutcome());
    assertEquals(2, decisions.get(0).getResponses().length);
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "a").getPlea());
    assertEquals(Plea.ACCEPT, getResponseForCohort(decisions.get(0), "b").getPlea());
    decisions.clear();
    vote(ballotId, "b", Plea.REJECT);
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
