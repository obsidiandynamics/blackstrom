package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.tracer.*;

final class PendingBallot {
  private final Nomination nomination;
  
  private final Map<String, Response> responses;
  
  private Verdict verdict = Verdict.COMMIT;
  
  private AbortReason abortReason;
  
  private Action action;
  
  PendingBallot(Nomination nomination) {
    this.nomination = nomination;
    responses = new HashMap<>(nomination.getCohorts().length);
  }
  
  Nomination getNomination() {
    return nomination;
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
  
  Action getAction() {
    return action;
  }

  void setAction(Action action) {
    this.action = action;
  }

  boolean castVote(Logger log, Vote vote) {
    final Response response = vote.getResponse();
    final Response existing = responses.put(response.getCohort(), response);
    if (existing != null) {
      if (DefaultMonitor.DEBUG) log.trace("Skipping redundant {} (already cast in current ballot)", vote);
      responses.put(existing.getCohort(), existing);
      return false;
    }
    
    final Pledge pledge = response.getPledge();
    if (pledge == Pledge.REJECT) {
      verdict = Verdict.ABORT;
      abortReason = AbortReason.REJECT;
      return true;
    } else if (pledge == Pledge.TIMEOUT) {
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
    return vote.getTimestamp() - nomination.getTimestamp() > nomination.getTtl();
  }
  
  boolean hasResponded(String cohort) {
    return responses.containsKey(cohort);
  }
  
  private boolean allResponsesPresent() {
    return responses.size() == nomination.getCohorts().length;
  }
}
