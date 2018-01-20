package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.tracer.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class DefaultMonitor implements Monitor {
  static final boolean DEBUG = false;
  
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMonitor.class);
  
  private Ledger ledger;
  
  private final Map<Object, PendingBallot> pending = new HashMap<>();
  
  private final Map<Object, Outcome> decided = new HashMap<>();
  
  private final String groupId;
  
  private final WorkerThread gcThread;
  
  private final int gcIntervalMillis;
  
  private final int outcomeLifetimeMillis;
  
  private long reapedSoFar;
  
  private final WorkerThread timeoutThread;
  
  private final int timeoutIntervalMillis;
  
  private final Object lock = new Object();
  
  private final ShardedTracer tracer = new ShardedTracer();
  
  public DefaultMonitor() {
    this(new DefaultMonitorOptions());
  }
  
  public DefaultMonitor(DefaultMonitorOptions options) {
    this.groupId = options.getGroupId();
    this.gcIntervalMillis = options.getGCInterval();
    this.outcomeLifetimeMillis = options.getOutcomeLifetime();
    this.timeoutIntervalMillis = options.getTimeoutInterval();
    
    gcThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(nameThread("gc"))
                     .withDaemon(true))
        .onCycle(this::gcCycle)
        .build();
    gcThread.start();
    
    timeoutThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(nameThread("timeout"))
                     .withDaemon(true))
        .onCycle(this::timeoutCycle)
        .build();
    timeoutThread.start();
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
    final List<Outcome> deathRow = new ArrayList<>();
    
    final List<Outcome> decidedCopy;
    synchronized (lock) {
      decidedCopy = new ArrayList<>(decided.values());
    }
    
    for (Outcome oucome : decidedCopy) {
      if (oucome.getTimestamp() < collectThreshold) {
        deathRow.add(oucome);
      }
    }
    
    if (! deathRow.isEmpty()) {
      for (Outcome outcome : deathRow) {
        synchronized (lock) {
          decided.remove(outcome.getBallotId());
        }
      }
      reapedSoFar += deathRow.size();
      
      LOG.debug("Reaped {} outcomes ({} so far), pending: {}, decided: {}", 
                deathRow.size(), reapedSoFar, pending.size(), decided.size());
    }
  }
  
  private void timeoutCycle(WorkerThread thread) throws InterruptedException {
    Thread.sleep(timeoutIntervalMillis);
    
    final List<PendingBallot> pendingCopy;
    synchronized (lock) {
      pendingCopy = new ArrayList<>(pending.values());
    }
    
    for (PendingBallot pending : pendingCopy) {
      final Nomination nomination = pending.getNomination();
      if (nomination.getTimestamp() + nomination.getTtl() < System.currentTimeMillis()) {
        for (String cohort : nomination.getCohorts()) {
          final boolean cohortResponded;
          synchronized (lock) {
            cohortResponded = pending.hasResponded(cohort);
          }
          
          if (! cohortResponded) {
            timeoutCohort(nomination, cohort);
          }
        }
      }
    }
  }
  
  private void timeoutCohort(Nomination nomination, String cohort) {
    LOG.debug("Timed out {} for cohort {}", nomination, cohort);
    append(new Vote(nomination.getBallotId(), new Response(cohort, Pledge.TIMEOUT, null)));
  }
  
  private void append(Message message) {
    try {
      ledger.append(message);
    } catch (Exception e) {
      LOG.warn("Error appending to ledger [message: " + message + "]", e);
    } 
  }
  
  public Map<Object, Outcome> getOutcomes() {
    final Map<Object, Outcome> decidedCopy;
    synchronized (lock) {
      decidedCopy = new HashMap<>(decided);
    }
    return Collections.unmodifiableMap(decidedCopy);
  }
  
  @Override
  public void onNomination(MessageContext context, Nomination nomination) {
    synchronized (lock) {
      if (decided.containsKey(nomination.getBallotId())) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already decided)", nomination);
        return;
      }
      
      final PendingBallot newBallot = new PendingBallot(nomination);
      final PendingBallot existingBallot = pending.put(nomination.getBallotId(), newBallot);
      if (existingBallot != null) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already pending)", nomination);
        pending.put(nomination.getBallotId(), existingBallot);
        return;
      } else {
        newBallot.setAction(tracer.begin(context, nomination));
      }
    }
    
    if (DEBUG) LOG.trace("Initiating ballot for {}", nomination);
  }

  @Override
  public void onVote(MessageContext context, Vote vote) {
    synchronized (lock) {
      final PendingBallot ballot = pending.get(vote.getBallotId());
      if (ballot != null) {
        if (DEBUG) LOG.trace("Received {}", vote);
        final boolean decided = ballot.castVote(LOG, vote);
        if (decided) {
          decideBallot(ballot);
        }
      } else if (decided.containsKey(vote.getBallotId())) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already decided)", vote);
      } else {
        if (DEBUG) LOG.trace("Missing pending ballot for vote {}", vote);
      }
    }
  }
  
  private void decideBallot(PendingBallot ballot) {
    if (DEBUG) LOG.trace("Decided ballot for {}: verdict: {}", ballot.getNomination(), ballot.getVerdict());
    final Object ballotId = ballot.getNomination().getBallotId();
    final Outcome outcome = new Outcome(ballotId, ballot.getVerdict(), ballot.getAbortReason(), ballot.getResponses());
    pending.remove(ballotId);
    decided.put(ballotId, outcome);
    append(outcome);
    ballot.getAction().complete();
  }
  
  @Override
  public void init(InitContext context) {
    ledger = context.getLedger();
  }
  
  @Override
  public void dispose() {
    gcThread.terminate();
    timeoutThread.terminate();
    gcThread.joinQuietly();
    timeoutThread.joinQuietly();
    tracer.dispose();
  }
}
