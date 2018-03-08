package com.obsidiandynamics.blackstrom.monitor;

import static junit.framework.TestCase.*;

import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.select.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class InlineMonitorTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private Manifold manifold;
  
  private InlineMonitor monitor;
  
  private final List<Proposal> proposals = new CopyOnWriteArrayList<>();
  
  private final List<Vote> votes = new CopyOnWriteArrayList<>();
  
  private final List<Outcome> outcomes = new CopyOnWriteArrayList<>();
  
  private final Timesert wait = Wait.SHORT;
  
  @After
  public void after() {
    cleanup();
  }
  
  private void cleanup() {
    if (manifold != null) {
      manifold.dispose();
    }
  }
  
  private void configure(DefaultMonitorConfig config) {
    final NullGroupMessageHandler downstreamHandler = (c, m) -> {
      Select.from(m)
      .whenInstanceOf(Proposal.class).then(proposals::add)
      .whenInstanceOf(Vote.class).then(votes::add)
      .whenInstanceOf(Outcome.class).then(outcomes::add)
      .otherwise(obj -> { throw new UnsupportedOperationException("Unsupported message " + obj); });
    };
    
    monitor = new InlineMonitor(config, downstreamHandler);
    
    manifold = Manifold.builder()
        .withLedger(new MultiNodeQueueLedger())
        .withFactors(monitor)
        .build();
  }
  
  @Test
  public void testProposalOutcome_oneCohort() {
    configure(new DefaultMonitorConfig().withMetadataEnabled(true));
    
    String ballotId;
    
    ballotId = UUID.randomUUID().toString();
    nominate(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.COMMIT, outcomes.get(0).getResolution());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals("ACCEPT", getResponseForCohort(outcomes.get(0), "a").getMetadata());
    outcomes.clear();
    
    ballotId = UUID.randomUUID().toString();
    nominate(ballotId, "a");
    vote(ballotId, "a", Intent.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertNotNull(outcomes.get(0).getMetadata());
    assertEquals(Resolution.ABORT, outcomes.get(0).getResolution());
    assertEquals(AbortReason.REJECT, outcomes.get(0).getAbortReason());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Intent.REJECT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals("REJECT", getResponseForCohort(outcomes.get(0), "a").getMetadata());
    outcomes.clear();
  }
  
  @Test
  public void testProposalOutcome_twoCohorts() {
    configure(new DefaultMonitorConfig());
    
    String ballotId;
    
    ballotId = UUID.randomUUID().toString();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.COMMIT, outcomes.get(0).getResolution());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    outcomes.clear();
    
    ballotId = UUID.randomUUID().toString();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Intent.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.ABORT, outcomes.get(0).getResolution());
    assertEquals(AbortReason.REJECT, outcomes.get(0).getAbortReason());
    assertNull(outcomes.get(0).getMetadata());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.REJECT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    outcomes.clear();
    
    ballotId = UUID.randomUUID().toString();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    vote(ballotId, "b", Intent.ACCEPT);
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.ABORT, outcomes.get(0).getResolution());
    assertEquals(AbortReason.REJECT, outcomes.get(0).getAbortReason());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Intent.REJECT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    outcomes.clear();
    
    ballotId = UUID.randomUUID().toString();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.REJECT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    vote(ballotId, "b", Intent.REJECT);
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.ABORT, outcomes.get(0).getResolution());
    assertEquals(AbortReason.REJECT, outcomes.get(0).getAbortReason());
    assertEquals(1, outcomes.get(0).getResponses().length);
    assertEquals(Intent.REJECT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    outcomes.clear();
  }
  
  @Test
  public void testDuplicateProposal_twoCohorts() {
    configure(new DefaultMonitorConfig()
              .withOutcomeLifetime(60_000)
              .withGCInterval(1));
    
    final String ballotId = UUID.randomUUID().toString();
    nominate(ballotId, "a", "b");
    nominate(ballotId, "a", "b", "c");
    vote(ballotId, "a", Intent.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    nominate(ballotId, "a", "b", "c");

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Intent.ACCEPT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.COMMIT, outcomes.get(0).getResolution());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    outcomes.clear();
    nominate(ballotId, "a", "b", "c");
    assertEquals(0, outcomes.size());

    wait.until(numTrackedOutcomesIs(1));
  }
  
  @Test
  public void testDuplicateVote_twoCohorts() {
    configure(new DefaultMonitorConfig());
    
    final String ballotId = UUID.randomUUID().toString();
    nominate(ballotId, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    vote(ballotId, "a", Intent.REJECT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    vote(ballotId, "b", Intent.ACCEPT);
    vote(ballotId, "b", Intent.TIMEOUT);

    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.COMMIT, outcomes.get(0).getResolution());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    outcomes.clear();
    vote(ballotId, "b", Intent.REJECT);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testVoteWithoutBallot() {
    configure(new DefaultMonitorConfig());
    
    final String ballotId = UUID.randomUUID().toString();
    vote(ballotId, "a", Intent.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testExplicitTimeout_twoCohorts() {
    configure(new DefaultMonitorConfig().withTimeoutInterval(1));
    
    final String ballotId = UUID.randomUUID().toString();
    final long startTimestamp = NanoClock.now();
    nominate(ballotId, 0, "a", "b");
    vote(ballotId, startTimestamp, "a", Intent.ACCEPT);
    
    wait.until(numVotesIsAtLeast(2));
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.ABORT, outcomes.get(0).getResolution());
    assertEquals(AbortReason.EXPLICIT_TIMEOUT, outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.TIMEOUT, getResponseForCohort(outcomes.get(0), "b").getIntent());
    
    // subsequent votes should have no effect
    vote(ballotId, "b", Intent.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(1, outcomes.size());
  }
  
  @Test
  public void testNoTimeout_twoCohorts() {
    configure(new DefaultMonitorConfig().withTimeoutInterval(1));
    
    final String ballotId = UUID.randomUUID().toString();
    nominate(ballotId, 10_000, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    
    vote(ballotId, "b", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.COMMIT, outcomes.get(0).getResolution());
    assertNull(outcomes.get(0).getAbortReason());
    assertEquals(2, outcomes.get(0).getResponses().length);
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "a").getIntent());
    assertEquals(Intent.ACCEPT, getResponseForCohort(outcomes.get(0), "b").getIntent());
  }
  
  @Test
  public void testImplicitTimeout_twoCohorts() {
    configure(new DefaultMonitorConfig()
              .withTimeoutInterval(60_000));
    
    final String ballotId = UUID.randomUUID().toString();
    nominate(ballotId, 1, "a", "b");
    vote(ballotId, NanoClock.now() + 1_000_000_000L, "a", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.ABORT, outcomes.get(0).getResolution());
    assertEquals(AbortReason.IMPLICIT_TIMEOUT, outcomes.get(0).getAbortReason());
  }

  private Runnable numVotesIsAtLeast(int size) {
    return () -> assertTrue("votes.size=" + votes.size(), votes.size() >= size);
  }
  
  private Runnable numOutcomesIs(int size) {
    return () -> assertEquals(size, outcomes.size());
  }
  
  private Runnable numTrackedOutcomesIs(int size) {
    return () -> assertEquals(size, monitor.getEngine().getOutcomes().size());
  }
  
  private Response getResponseForCohort(Outcome outcome, String cohort) {
    return Arrays.stream(outcome.getResponses()).filter(r -> r.getCohort().equals(cohort)).findAny().get();
  }

  private void nominate(String ballotId, String... cohorts) {
    nominate(ballotId, Integer.MAX_VALUE, cohorts);
  }

  private void nominate(String ballotId, int ttl, String... cohorts) {
    manifold.getLedger().append(new Proposal(ballotId.toString(), cohorts, null, ttl));
  }

  private void vote(String ballotId, String cohort, Intent intent) {
    vote(ballotId, 0, cohort, intent);
  }

  private void vote(String ballotId, long timestamp, String cohort, Intent intent) {
    manifold.getLedger().append(new Vote(ballotId.toString(), timestamp, new Response(cohort, intent, intent.name())));
  }
}
