package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.nanoclock.*;
import com.obsidiandynamics.nodequeue.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;
import com.obsidiandynamics.zerolog.*;

public final class MonitorEngine implements Disposable {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
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
  
  private int logLevel = LogLevel.TRACE;
  
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
  
  public MonitorEngine withLogLevel(int logLevel) {
    this.logLevel = logLevel;
    return this;
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
        final int _reaped = reaped;
        zlg.i("Reaped %,d outcomes (%,d so far), pending: %,d, decided: %,d", 
              z -> z.arg(_reaped).arg(reapedSoFar).arg(pending::size).arg(decided::size));
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
    zlg.level(logLevel).format("Timed out %s for cohort %s").arg(proposal).arg(cohort).log();
    append(new Vote(proposal.getXid(), new Response(cohort, Intent.TIMEOUT, null))
           .inResponseTo(proposal).withSource(groupId));
  }
  
  private void append(Vote vote) {
    action.appendVote(vote, (id, x) -> {
      if (x != null) zlg.w("Error appending to ledger [message: %s]", z -> z.arg(vote).threw(x));
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
      final PendingBallot existingBallot = pending.put(proposal.getXid(), newBallot);
      if (existingBallot != null) {
        zlg.level(logLevel).format("Skipping redundant %s (ballot already pending)").arg(proposal).log();
        pending.put(proposal.getXid(), existingBallot);
        return;
      } else {
        newBallot.setConfirmation(context.begin(proposal));
      }
    }
    
    zlg.level(logLevel).format("Initiating ballot for %s").arg(proposal).log();
  }

  public void onVote(MessageContext context, Vote vote) {
    synchronized (messageLock) {
      final PendingBallot ballot = pending.get(vote.getXid());
      if (ballot != null) {
        zlg.level(logLevel).format("Received %s").arg(vote).log();
        final boolean decided = ballot.castVote(vote, zlg, logLevel);
        if (decided) {
          decideBallot(ballot);
        }
      } else {
        zlg.level(logLevel).format("Missing pending ballot for vote %s").arg(vote).log();
      }
    }
  }
  
  private void decideBallot(PendingBallot ballot) {
    zlg.level(logLevel).format("Decided ballot for %s: resolution: %s").arg(ballot::getProposal).arg(ballot::getResolution).log();
    final Proposal proposal = ballot.getProposal();
    final String xid = proposal.getXid();
    final Object metadata = metadataEnabled ? new OutcomeMetadata(proposal.getTimestamp()) : null;
    final Outcome outcome = new Outcome(xid, ballot.getResolution(), ballot.getAbortReason(), ballot.getResponses(), metadata)
        .inResponseTo(proposal).withSource(groupId);
    pending.remove(xid);
    if (trackingEnabled) {
      additions.add(outcome);
    }
    action.appendOutcome(outcome, (id, x) -> {
      if (x == null) {
        ballot.getConfirmation().confirm();
      } else {
        zlg.w("Error appending to ledger [message: %s]", z -> z.arg(outcome).threw(x));
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
