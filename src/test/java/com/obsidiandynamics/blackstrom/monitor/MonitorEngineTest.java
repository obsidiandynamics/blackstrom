package com.obsidiandynamics.blackstrom.monitor;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.mockito.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class MonitorEngineTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final Timesert wait = Wait.SHORT;
  
  private final InitContext initContext = new DefaultInitContext(new Ledger() {
    @Override public void attach(MessageHandler handler) {
      ledger.attach(handler);
    }

    @Override public void append(Message message, AppendCallback callback) {
      ledger.append(message, callback);
    }
    
    @Override public void confirm(Object handlerId, MessageId messageId) {
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
      (m.getMessageType() == MessageType.OUTCOME ? outcomes : votes).add(Cast.from(m));
    });
    ledger.init();
    context = new DefaultMessageContext(ledger, null, NopRetention.getInstance());
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
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig()));
    assertEquals("monitor", monitor.getGroupId());
  }
  
  @Test
  public void testProposalOutcome_oneCohort() {
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig().withMetadataEnabled(true)));
    
    String ballotId;
    
    ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a");
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
    propose(ballotId, "a");
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
    String ballotId;
    
    ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a", "b");
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
    propose(ballotId, "a", "b");
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
    propose(ballotId, "a", "b");
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
    propose(ballotId, "a", "b");
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
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig()
                                         .withOutcomeLifetime(60_000)
                                         .withGCInterval(1)));
    
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a", "b");
    propose(ballotId, "a", "b", "c");
    vote(ballotId, "a", Intent.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    propose(ballotId, "a", "b", "c");

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
    propose(ballotId, "a", "b", "c");
    assertEquals(0, outcomes.size());

    wait.until(numTrackedOutcomesIs(1));
  }
  
  @Test
  public void testDuplicateVote_twoCohorts() {
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a", "b");
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
    final String ballotId = UUID.randomUUID().toString();
    vote(ballotId, "a", Intent.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testGCNoReap() {
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig()
                                         .withOutcomeLifetime(60_000)
                                         .withGCInterval(1)));
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);
    
    monitor.getEngine().gc();
    wait.until(numOutcomesIs(1));
    wait.until(numTrackedOutcomesIs(1));
  }
  
  @Test
  public void testGCReap() {
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig().withOutcomeLifetime(1).withGCInterval(1)));
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    
    wait.until(() -> assertEquals(1, monitor.getEngine().getNumReapedOutcomes()));
    wait.until(numTrackedOutcomesIs(0));
  }
  
  private static class TestLedgerException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }
  
  @Test
  public void testAppendError() {
    setLedger(Mockito.mock(Ledger.class));
    Mockito.doThrow(TestLedgerException.class).when(ledger).append(Mockito.any());
    ledger.attach((NullGroupMessageHandler) (c, m) -> outcomes.add((Outcome) m));
    ledger.init();
    context = new DefaultMessageContext(ledger, null, NopRetention.getInstance());
    
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);

    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
  }
  
  @Test
  public void testExplicitTimeout_twoCohorts() {
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig().withTimeoutInterval(1)));
    
    final String ballotId = UUID.randomUUID().toString();
    final long startTimestamp = NanoClock.now();
    propose(ballotId, 0, "a", "b");
    vote(ballotId, startTimestamp, "a", Intent.ACCEPT);
    
    wait.until(numVotesIsAtLeast(1));
    assertEquals(0, outcomes.size());
    assertEquals(ballotId, votes.get(0).getBallotId());
    assertEquals(Intent.TIMEOUT, votes.get(0).getResponse().getIntent());
    
    // feed the timeout back into the monitor - should produce a rejection
    vote(ballotId, "b", Intent.TIMEOUT);
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
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig().withTimeoutInterval(1)));
    
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, 10_000, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    
    TestSupport.sleep(10);
    assertEquals(0, outcomes.size());
    assertEquals(0, votes.size());
    
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
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig()
                                         .withTimeoutInterval(60_000)));
    
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, 1, "a", "b");
    vote(ballotId, NanoClock.now() + 1_000_000_000L, "a", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    assertEquals(1, outcomes.size());
    assertEquals(ballotId, outcomes.get(0).getBallotId());
    assertEquals(Resolution.ABORT, outcomes.get(0).getResolution());
    assertEquals(AbortReason.IMPLICIT_TIMEOUT, outcomes.get(0).getAbortReason());
  }
  
  @Test(expected=IllegalStateException.class)
  public void testNoTracking_getOutcomes() {
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig()
                                         .withTrackingEnabled(false)));
    
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a", "b");
    vote(ballotId, "a", Intent.ACCEPT);
    vote(ballotId, "b", Intent.ACCEPT);
    
    wait.until(numOutcomesIs(1));
    monitor.getEngine().getOutcomes();
  }
  
  @Test(expected=IllegalStateException.class)
  public void testNoTracking_getNumReapedOutcomes() {
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig()
                                         .withTrackingEnabled(false)));
    monitor.getEngine().getNumReapedOutcomes();
  }
  
  @Test
  public void testTimeoutVoteBadLedger() {
    setMonitorAndInit(new DefaultMonitor(new MonitorEngineConfig().withTimeoutInterval(1)));
    final Ledger ledger = mock(Ledger.class);
    setLedger(ledger);
    ledger.init();
    
    final AtomicBoolean responded = new AtomicBoolean();
    doAnswer(invocation -> {
      final AppendCallback callback = invocation.getArgument(1);
      callback.onAppend(null, new Exception("simulated append error"));
      responded.set(true);
      return null;
    }).when(ledger).append(any(), any());
    
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, 0, "a");
    
    wait.untilTrue(responded::get);
  }
  
  @Test
  public void testOutcomeBadLedger() {
    final Ledger ledger = mock(Ledger.class);
    setLedger(ledger);
    ledger.init();
    
    final AtomicBoolean responded = new AtomicBoolean();
    doAnswer(invocation -> {
      final AppendCallback callback = invocation.getArgument(1);
      callback.onAppend(null, new Exception("simulated append error"));
      responded.set(true);
      return null;
    }).when(ledger).append(any(), any());
    
    final String ballotId = UUID.randomUUID().toString();
    propose(ballotId, "a");
    vote(ballotId, "a", Intent.ACCEPT);
    
    wait.untilTrue(responded::get);
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

  private void propose(String ballotId, String... cohorts) {
    propose(ballotId, Integer.MAX_VALUE, cohorts);
  }

  private void propose(String ballotId, int ttl, String... cohorts) {
    monitor.onProposal(context, new Proposal(ballotId.toString(), cohorts, null, ttl));
  }

  private void vote(String ballotId, String cohort, Intent intent) {
    vote(ballotId, 0, cohort, intent);
  }

  private void vote(String ballotId, long timestamp, String cohort, Intent intent) {
    monitor.onVote(context, new Vote(ballotId.toString(), timestamp, new Response(cohort, intent, intent.name())));
  }
}
