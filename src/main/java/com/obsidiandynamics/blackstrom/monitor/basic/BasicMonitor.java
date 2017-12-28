package com.obsidiandynamics.blackstrom.monitor.basic;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class BasicMonitor implements Monitor {
  static final boolean DEBUG = false;
  
  private static final Logger LOG = LoggerFactory.getLogger(BasicMonitor.class);
  
  private final Map<Object, PendingBallot> pending = new HashMap<>();
  
  private final Map<Object, Decision> decided = new HashMap<>();
  
  private final String nodeId = getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
  
  @Override
  public void onNomination(VotingContext context, Nomination nomination) {
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
    
    if (DEBUG) LOG.trace("Initiating ballot for {}", nomination);
  }

  @Override
  public void onVote(VotingContext context, Vote vote) {
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
}
