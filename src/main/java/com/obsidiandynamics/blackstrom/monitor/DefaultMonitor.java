package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class DefaultMonitor implements Monitor {
  static final boolean DEBUG = false;
  
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMonitor.class);
  
  private Ledger ledger;
  
  private final Map<Object, PendingBallot> pending = new HashMap<>();
  
  private final Object trackerLock = new Object();
  private final List<Outcome> decided = new LinkedList<>();
  private final NodeQueue<Outcome> additions = new NodeQueue<>();
  private final QueueConsumer<Outcome> additionsConsumer = additions.consumer();
  
  private final String groupId;
  
  private final WorkerThread gcThread;
  
  private final boolean trackingEnabled;
  
  private final int gcIntervalMillis;
  
  private final int outcomeLifetimeMillis;
  
  private long reapedSoFar;
  
  private final WorkerThread timeoutThread;
  
  private final int timeoutIntervalMillis;
  
  private final Object messageLock = new Object();
  
  private final ShardedFlow flow = new ShardedFlow();
  
  public DefaultMonitor() {
    this(new DefaultMonitorOptions());
  }
  
  public DefaultMonitor(DefaultMonitorOptions options) {
    this.groupId = options.getGroupId();
    this.trackingEnabled = options.isTrackingEnabled();
    this.gcIntervalMillis = options.getGCInterval();
    this.outcomeLifetimeMillis = options.getOutcomeLifetime();
    this.timeoutIntervalMillis = options.getTimeoutInterval();
    
    if (trackingEnabled) {
      gcThread = WorkerThread.builder()
          .withOptions(new WorkerOptions()
                       .withName(nameThread("gc"))
                       .withDaemon(true))
          .onCycle(this::gcCycle)
          .buildAndStart();
    } else {
      gcThread = null;
    }
    
    timeoutThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(nameThread("timeout"))
                     .withDaemon(true))
        .onCycle(this::timeoutCycle)
        .buildAndStart();
  }
  
  private String nameThread(String role) {
    return DefaultMonitor.class.getSimpleName() + "-" + role + "-" + Integer.toHexString(System.identityHashCode(this));
  }

  @Override
  public String getGroupId() {
    return groupId;
  }
  
  private void gcCycle(WorkerThread thread) throws InterruptedException {
    Thread.sleep(gcIntervalMillis);
    
    final long collectThreshold = System.currentTimeMillis() - outcomeLifetimeMillis;
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
      LOG.debug("Reaped {} outcomes ({} so far), pending: {}, decided: {}", 
                reaped, reapedSoFar, pending.size(), decided.size());
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
      if (proposal.getTimestamp() + proposal.getTtl() < System.currentTimeMillis()) {
        for (String cohort : proposal.getCohorts()) {
          final boolean cohortResponded;
          synchronized (messageLock) {
            cohortResponded = pending.hasResponded(cohort);
          }
          
          if (! cohortResponded) {
            timeoutCohort(proposal, cohort);
          }
        }
      }
    }
  }
  
  private void timeoutCohort(Proposal proposal, String cohort) {
    LOG.debug("Timed out {} for cohort {}", proposal, cohort);
    append(new Vote(proposal.getBallotId(), new Response(cohort, Intent.TIMEOUT, null)).inResponseTo(proposal));
  }
  
  private void append(Message message) {
    ledger.append(message, (id, x) -> {
      if (x != null) LOG.warn("Error appending to ledger [message: " + message + "]", x);
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
  
  @Override
  public void onProposal(MessageContext context, Proposal proposal) {
    synchronized (messageLock) {
      final PendingBallot newBallot = new PendingBallot(proposal);
      final PendingBallot existingBallot = pending.put(proposal.getBallotId(), newBallot);
      if (existingBallot != null) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already pending)", proposal);
        pending.put(proposal.getBallotId(), existingBallot);
        return;
      } else {
        newBallot.setConfirmation(flow.begin(context, proposal));
      }
    }
    
    if (DEBUG) LOG.trace("Initiating ballot for {}", proposal);
  }

  @Override
  public void onVote(MessageContext context, Vote vote) {
    synchronized (messageLock) {
      final PendingBallot ballot = pending.get(vote.getBallotId());
      if (ballot != null) {
        if (DEBUG) LOG.trace("Received {}", vote);
        final boolean decided = ballot.castVote(LOG, vote);
        if (decided) {
          decideBallot(ballot);
        }
      } else {
        if (DEBUG) LOG.trace("Missing pending ballot for vote {}", vote);
      }
    }
  }
  
  private void decideBallot(PendingBallot ballot) {
    if (DEBUG) LOG.trace("Decided ballot for {}: verdict: {}", ballot.getProposal(), ballot.getVerdict());
    final Proposal proposal = ballot.getProposal();
    final String ballotId = proposal.getBallotId();
    final Outcome outcome = new Outcome(ballotId, ballot.getVerdict(), ballot.getAbortReason(), ballot.getResponses())
        .inResponseTo(proposal);
    pending.remove(ballotId);
    if (trackingEnabled) {
      additions.add(outcome);
    }
    ledger.append(outcome, (id, x) -> {
      if (x == null) {
        ballot.getConfirmation().confirm();
      } else {
        LOG.warn("Error appending to ledger [message: " + outcome + "]", x);
      }
    });
  }
  
  @Override
  public void init(InitContext context) {
    ledger = context.getLedger();
  }
  
  @Override
  public void dispose() {
    if (trackingEnabled) gcThread.terminate();
    timeoutThread.terminate();
    if (trackingEnabled) gcThread.joinQuietly();
    timeoutThread.joinQuietly();
    flow.dispose();
  }
}
