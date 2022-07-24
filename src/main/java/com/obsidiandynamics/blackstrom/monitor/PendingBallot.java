package com.obsidiandynamics.blackstrom.monitor;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.flow.*;
import com.obsidiandynamics.zerolog.*;

final class PendingBallot {
  private final Proposal proposal;
  
  private final Map<String, Response> responses;
  
  private final Confirmation confirmation;
  
  private final AtomicBoolean confirmed = new AtomicBoolean();
  
  private Resolution resolution = Resolution.COMMIT;
  
  private AbortReason abortReason;
  
  private Set<String> explicitTimeoutsSent;
  
  PendingBallot(Proposal proposal, Confirmation confirmation) {
    this.proposal = mustExist(proposal);
    this.confirmation = mustExist(confirmation);
    responses = new HashMap<>(proposal.getCohorts().length, 1f);
  }
  
  Proposal getProposal() {
    return proposal;
  }
  
  Resolution getResolution() {
    return resolution;
  }
  
  AbortReason getAbortReason() {
    return abortReason;
  }
  
  Response[] getResponses() {
    final Collection<Response> responses = this.responses.values();
    return responses.toArray(new Response[0]);
  }
  
  void confirm() {
    if (confirmed.compareAndSet(false, true)) {
      confirmation.confirm();
    }
  }

  boolean castVote(Zlg zlg, Vote vote) {
    final Response response = vote.getResponse();
    final Response existing = responses.put(response.getCohort(), response);
    if (existing != null) {
      zlg.t("Skipping redundant %s (already cast in current ballot)", z -> z.arg(vote));
      responses.put(existing.getCohort(), existing);
      return false;
    }
    
    final Intent intent = response.getIntent();
    if (intent == Intent.REJECT) {
      resolution = Resolution.ABORT;
      abortReason = AbortReason.REJECT;
      return true;
    } else if (intent == Intent.TIMEOUT) {
      resolution = Resolution.ABORT;
      abortReason = AbortReason.EXPLICIT_TIMEOUT;
      return true;
    } else if (hasLapsed(vote)) {
      resolution = Resolution.ABORT;
      abortReason = AbortReason.IMPLICIT_TIMEOUT;
      return true;
    }
    
    return allResponsesPresent();
  }
  
  private boolean hasLapsed(Vote vote) {
    return vote.getTimestamp() - proposal.getTimestamp() > proposal.getTtl() * 1_000_000L;
  }
  
  boolean hasResponded(String cohort) {
    return responses.containsKey(cohort);
  }
  
  private boolean allResponsesPresent() {
    return responses.size() == proposal.getCohorts().length;
  }
  
  boolean tryEnqueueExplicitTimeout(String cohort) {
    if (explicitTimeoutsSent == null) {
      explicitTimeoutsSent = new HashSet<>();
    }
    return explicitTimeoutsSent.add(cohort);
  }
}
