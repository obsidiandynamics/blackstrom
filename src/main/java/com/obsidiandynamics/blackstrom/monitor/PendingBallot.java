package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.model.*;

final class PendingBallot {
  private final Proposal proposal;
  
  private final Map<String, Response> responses;
  
  private Verdict verdict = Verdict.COMMIT;
  
  private AbortReason abortReason;
  
  private Confirmation confirmation;
  
  PendingBallot(Proposal proposal) {
    this.proposal = proposal;
    responses = new HashMap<>(proposal.getCohorts().length);
  }
  
  Proposal getProposal() {
    return proposal;
  }
  
  Verdict getVerdict() {
    return verdict;
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

  boolean castVote(Logger log, Vote vote) {
    final Response response = vote.getResponse();
    final Response existing = responses.put(response.getCohort(), response);
    if (existing != null) {
      if (DefaultMonitor.DEBUG) log.trace("Skipping redundant {} (already cast in current ballot)", vote);
      responses.put(existing.getCohort(), existing);
      return false;
    }
    
    final Intent intent = response.getIntent();
    if (intent == Intent.REJECT) {
      verdict = Verdict.ABORT;
      abortReason = AbortReason.REJECT;
      return true;
    } else if (intent == Intent.TIMEOUT) {
      verdict = Verdict.ABORT;
      abortReason = AbortReason.EXPLICIT_TIMEOUT;
      return true;
    } else if (hasLapsed(vote)) {
      verdict = Verdict.ABORT;
      abortReason = AbortReason.IMPLICIT_TIMEOUT;
      return true;
    }
    
    return allResponsesPresent();
  }
  
  private boolean hasLapsed(Vote vote) {
    return vote.getTimestamp() - proposal.getTimestamp() > proposal.getTtl();
  }
  
  boolean hasResponded(String cohort) {
    return responses.containsKey(cohort);
  }
  
  private boolean allResponsesPresent() {
    return responses.size() == proposal.getCohorts().length;
  }
}
