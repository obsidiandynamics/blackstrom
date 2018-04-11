package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.nanoclock.*;
import com.obsidiandynamics.nodequeue.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;

public final class MonitorEngine implements Disposable {
  static final boolean DEBUG = false;
  
  private static final Logger log = LoggerFactory.getLogger(MonitorEngine.class);
  
  private final Map<Object, PendingBallot> pending = new HashMap<>();
  
  private final Object trackerLock = new Object();
  private final List<Outcome> decided = new LinkedList<>();
  private final NodeQueue<Outcome> additions = new NodeQueue<>();
  private final QueueConsumer<Outcome> additionsConsumer = additions.consumer();
  
  private final String groupId;
  
  private final WorkerThread gcThread;
  
  private final boolean trackingEnabled;
  
  private final int gcIntervalMillis;
  
  private final Object gcLock = new Object();
  
  private final int outcomeLifetimeMillis;
  
  private long reapedSoFar;
  
  private final WorkerThread timeoutThread;
  
  private final int timeoutIntervalMillis;
  
  private final Object messageLock = new Object();
  
  private final boolean metadataEnabled;
  
  private final MonitorAction action;
  
  public MonitorEngine(MonitorAction action, String groupId, MonitorEngineConfig config) {
    this.groupId = groupId;
    trackingEnabled = config.isTrackingEnabled();
    gcIntervalMillis = config.getGCInterval();
    outcomeLifetimeMillis = config.getOutcomeLifetime();
    timeoutIntervalMillis = config.getTimeoutInterval();
    metadataEnabled = config.isMetadataEnabled();
    this.action = action;
    
    if (trackingEnabled) {
      gcThread = WorkerThread.builder()
          .withOptions(new WorkerOptions()
                       .daemon()
                       .withName(MonitorEngine.class, groupId, "gc", Integer.toHexString(System.identityHashCode(this))))
          .onCycle(this::gcCycle)
          .buildAndStart();
    } else {
      gcThread = null;
    }
    
    timeoutThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .daemon()
                     .withName(MonitorEngine.class, groupId, "timeout", Integer.toHexString(System.identityHashCode(this))))
        .onCycle(this::timeoutCycle)
        .buildAndStart();
  }
  
  private void gcCycle(WorkerThread thread) throws InterruptedException {
    Thread.sleep(gcIntervalMillis);
    gc();
  }
  
  void gc() {
    synchronized (gcLock) {
      final long collectThreshold = NanoClock.now() - outcomeLifetimeMillis * 1_000_000L;
      int reaped = 0;
      synchronized (trackerLock) {
        for (Iterator<Outcome> outcomesIt = decided.iterator(); outcomesIt.hasNext();) {
          final Outcome outcome = outcomesIt.next();
          if (outcome.getTimestamp() < collectThreshold) {
            outcomesIt.remove();
            reaped++;
          }
        }
        
        for (;;) {
          final Outcome addition = additionsConsumer.poll();
          if (addition != null) {
            decided.add(addition);
          } else {
            break;
          }
        }
      }
      
      if (reaped != 0) {
        reapedSoFar += reaped;
        log.debug("Reaped {} outcomes ({} so far), pending: {}, decided: {}", 
                  reaped, reapedSoFar, pending.size(), decided.size());
      }
    }
  }
  
  private void timeoutCycle(WorkerThread thread) throws InterruptedException {
    Thread.sleep(timeoutIntervalMillis);
    
    final List<PendingBallot> pendingCopy;
    synchronized (messageLock) {
      pendingCopy = new ArrayList<>(pending.values());
    }
    
    for (PendingBallot pending : pendingCopy) {
      final Proposal proposal = pending.getProposal();
      if (proposal.getTimestamp() + proposal.getTtl() * 1_000_000L < NanoClock.now()) {
        for (String cohort : proposal.getCohorts()) {
          final boolean cohortResponded;
          synchronized (messageLock) {
            cohortResponded = pending.hasResponded(cohort);
          }
          
          if (! cohortResponded && pending.tryEnqueueExplicitTimeout(cohort)) {
            timeoutCohort(proposal, cohort);
          }
        }
      }
    }
  }
  
  private void timeoutCohort(Proposal proposal, String cohort) {
    log.debug("Timed out {} for cohort {}", proposal, cohort);
    append(new Vote(proposal.getBallotId(), new Response(cohort, Intent.TIMEOUT, null))
           .inResponseTo(proposal).withSource(groupId));
  }
  
  private void append(Vote vote) {
    action.appendVote(vote, (id, x) -> {
      if (x != null) log.warn("Error appending to ledger [message: " + vote + "]", x);
    });
  }
  
  public List<Outcome> getOutcomes() {
    if (! trackingEnabled) throw new IllegalStateException("Tracking is not enabled");
    
    final List<Outcome> decidedCopy;
    synchronized (trackerLock) {
      decidedCopy = new ArrayList<>(decided);
    }
    return Collections.unmodifiableList(decidedCopy);
  }
  
  public long getNumReapedOutcomes() {
    if (! trackingEnabled) throw new IllegalStateException("Tracking is not enabled");
    
    return reapedSoFar;
  }
  
  public void onProposal(MessageContext context, Proposal proposal) {
    synchronized (messageLock) {
      final PendingBallot newBallot = new PendingBallot(proposal);
      final PendingBallot existingBallot = pending.put(proposal.getBallotId(), newBallot);
      if (existingBallot != null) {
        if (DEBUG) log.trace("Skipping redundant {} (ballot already pending)", proposal);
        pending.put(proposal.getBallotId(), existingBallot);
        return;
      } else {
        newBallot.setConfirmation(context.begin(proposal));
      }
    }
    
    if (DEBUG) log.trace("Initiating ballot for {}", proposal);
  }

  public void onVote(MessageContext context, Vote vote) {
    synchronized (messageLock) {
      final PendingBallot ballot = pending.get(vote.getBallotId());
      if (ballot != null) {
        if (DEBUG) log.trace("Received {}", vote);
        final boolean decided = ballot.castVote(log, vote);
        if (decided) {
          decideBallot(ballot);
        }
      } else {
        if (DEBUG) log.trace("Missing pending ballot for vote {}", vote);
      }
    }
  }
  
  private void decideBallot(PendingBallot ballot) {
    if (DEBUG) log.trace("Decided ballot for {}: resolution: {}", ballot.getProposal(), ballot.getResolution());
    final Proposal proposal = ballot.getProposal();
    final String ballotId = proposal.getBallotId();
    final Object metadata = metadataEnabled ? new OutcomeMetadata(proposal.getTimestamp()) : null;
    final Outcome outcome = new Outcome(ballotId, ballot.getResolution(), ballot.getAbortReason(), ballot.getResponses(), metadata)
        .inResponseTo(proposal).withSource(groupId);
    pending.remove(ballotId);
    if (trackingEnabled) {
      additions.add(outcome);
    }
    action.appendOutcome(outcome, (id, x) -> {
      if (x == null) {
        ballot.getConfirmation().confirm();
      } else {
        log.warn("Error appending to ledger [message: " + outcome + "]", x);
      }
    });
  }
    
  @Override
  public void dispose() {
    Terminator.blank()
    .add(Optional.ofNullable(gcThread))
    .add(timeoutThread)
    .terminate()
    .joinSilently();
  }
}
