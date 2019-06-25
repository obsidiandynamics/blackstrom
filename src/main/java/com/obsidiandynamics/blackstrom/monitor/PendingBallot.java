package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;

import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.flow.*;
import com.obsidiandynamics.zerolog.*;

final class PendingBallot {
  private final Proposal proposal;
  
  private final Map<String, Response> responses;
  
  private Resolution resolution = Resolution.COMMIT;
  
  private AbortReason abortReason;
  
  private Confirmation confirmation;
  
  private Set<String> explicitTimeoutsSent;
  
  PendingBallot(Proposal proposal) {
    this.proposal = proposal;
    responses = new HashMap<>(proposal.getCohorts().length);
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
    final Response[] array = responses.toArray(new Response[responses.size()]);
    return array;
  }
  
  Confirmation getConfirmation() {
    return confirmation;
  }

  void setConfirmation(Confirmation confirmation) {
    this.confirmation = confirmation;
  }

  boolean castVote(Vote vote, Zlg zlg, int logLevel) {
    final Response response = vote.getResponse();
    final Response existing = responses.put(response.getCohort(), response);
    if (existing != null) {
      zlg.level(logLevel).format("Skipping redundant %s (already cast in current ballot)").arg(vote).log();
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
