package com.obsidiandynamics.blackstrom.monitor.basic;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class BasicMonitor implements Monitor {
  static final boolean DEBUG = false;
  
  private static final Logger LOG = LoggerFactory.getLogger(BasicMonitor.class);
  
  private final Map<Object, PendingBallot> pending = new HashMap<>();
  
  private final Map<Object, Decision> decided = new HashMap<>();
  
  private final String nodeId = getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
  
  private final int gcIntervalMillis = 1_000;
  
  private final int decisionLifetimeMillis = 1_000;
  
  private final WorkerThread gc;
  
  private final Object lock = new Object();
  
  private long reapedSoFar;
  
  public BasicMonitor() {
    gc = WorkerThread.builder()
        .withOptions(new WorkerOptions().withName("gc-" + nodeId).withDaemon(true))
        .withWorker(this::gc)
        .build();
    gc.start();
  }
  
  private void gc(WorkerThread thread) throws InterruptedException {
    Thread.sleep(gcIntervalMillis);
    
    final long collectThreshold = System.currentTimeMillis() - decisionLifetimeMillis;
    List<Decision> deathRow = null;
    
    final List<Decision> decidedCopy;
    synchronized (lock) {
      decidedCopy = new ArrayList<>(decided.values());
    }
    
    for (Decision decision : decidedCopy) {
      if (decision.getTimestamp() < collectThreshold) {
        if (deathRow == null) deathRow = new ArrayList<>();
        deathRow.add(decision);
      }
    }
    
    if (deathRow != null) {
      for (Decision decision : deathRow) {
        synchronized (lock) {
          decided.remove(decision.getBallotId());
        }
      }
      reapedSoFar += deathRow.size();
      
      LOG.debug("Reaped {} decisions ({} so far), pending: {}, decided: {}", 
                deathRow.size(), reapedSoFar, pending.size(), decided.size());
    }
  }
  
  @Override
  public void onNomination(VotingContext context, Nomination nomination) {
    synchronized (lock) {
      if (decided.containsKey(nomination.getBallotId())) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already decided)", nomination);
        return;
      }
      
      final PendingBallot existing = pending.put(nomination.getBallotId(), new PendingBallot(nomination));
      if (existing != null) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already pending)", nomination);
        pending.put(nomination.getBallotId(), existing);
        return;
      }
    }
    
    if (DEBUG) LOG.trace("Initiating ballot for {}", nomination);
  }

  @Override
  public void onVote(VotingContext context, Vote vote) {
    synchronized (lock) {
      final PendingBallot ballot = pending.get(vote.getBallotId());
      if (ballot != null) {
        if (DEBUG) LOG.trace("Received {}", vote);
        final boolean decided = ballot.castVote(LOG, vote);
        if (decided) {
          decideBallot(context, ballot);
        }
      } else if (decided.containsKey(vote.getBallotId())) {
        if (DEBUG) LOG.trace("Skipping redundant {} (ballot already decided)", vote);
      } else {
        LOG.warn("Missing pending ballot for vote {}", vote);
      }
    }
  }
  
  private void decideBallot(VotingContext context, PendingBallot ballot) {
    if (DEBUG) LOG.trace("Decided ballot for {}: outcome: {}", ballot.getNomination(), ballot.getOutcome());
    final Object ballotId = ballot.getNomination().getBallotId();
    final Decision decision = new Decision(ballotId, ballotId, nodeId, ballot.getOutcome(), ballot.getResponses());
    pending.remove(ballotId);
    decided.put(ballotId, decision);
    try {
      context.getLedger().append(decision);
    } catch (Exception e) {
      LOG.warn("Error appending to ledger {}", e);
    }
  }
  
  @Override
  public void dispose() {
    gc.terminate();
    gc.joinQuietly();
  }
}
