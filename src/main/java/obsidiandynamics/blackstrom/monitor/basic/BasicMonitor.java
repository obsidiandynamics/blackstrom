package obsidiandynamics.blackstrom.monitor.basic;

import java.util.*;

import org.slf4j.*;

import obsidiandynamics.blackstrom.handler.*;
import obsidiandynamics.blackstrom.model.*;
import obsidiandynamics.blackstrom.monitor.*;

public final class BasicMonitor implements Monitor {
  private static final Logger LOG = LoggerFactory.getLogger(BasicMonitor.class);
  
  private final Map<Object, PendingBallot> pending = new HashMap<>();
  
  private final Map<Object, Decision> decided = new HashMap<>();
  
  private final String nodeId = getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
  
  private long messageId;

  @Override
  public void onNomination(VotingContext context, Nomination nomination) {
    if (decided.containsKey(nomination.getBallotId())) {
      LOG.trace("Skipping redundant {} (ballot already decided)", nomination);
      return;
    }
    
    final PendingBallot existing = pending.put(nomination.getBallotId(), new PendingBallot(nomination));
    if (existing != null) {
      LOG.trace("Skipping redundant {} (ballot already pending)", nomination);
      pending.put(nomination.getBallotId(), existing);
      return;
    }
    
    LOG.trace("Initiating ballot for {}", nomination);
  }

  @Override
  public void onVote(VotingContext context, Vote vote) {
    final PendingBallot ballot = pending.get(vote.getBallotId());
    if (ballot != null) {
      LOG.trace("Received {}", vote);
      final boolean decided = ballot.castVote(LOG, vote);
      if (decided) {
        decideBallot(context, ballot);
      }
    } else if (decided.containsKey(vote.getBallotId())) {
      LOG.trace("Skipping redundant {} (ballot already decided)", vote);
    } else {
      LOG.warn("Missing pending ballot for vote {}", vote);
    }
  }
  
  private void decideBallot(VotingContext context, PendingBallot ballot) {
    LOG.trace("Decided ballot for {}: outcome: {}", ballot.getNomination(), ballot.getOutcome());
    final Object ballotId = ballot.getNomination().getBallotId();
    final Decision decision = new Decision(messageId++, ballotId, nodeId, ballot.getOutcome(), ballot.getResponses());
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
  }
}
