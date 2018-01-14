package com.obsidiandynamics.blackstrom.monitor;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.model.*;

final class PendingBallot {
  private final Nomination nomination;
  
  private final Map<String, Response> responses;
  
  private Verdict verdict = Verdict.COMMIT;
  
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
  
  Response[] getResponses() {
    final Collection<Response> responses = this.responses.values();
    final Response[] array = responses.toArray(new Response[responses.size()]);
    return array;
  }
  
  boolean castVote(Logger log, Vote vote) {
    final Response response = vote.getResponse();
    final Response existing = responses.put(response.getCohort(), response);
    if (existing != null) {
      if (DefaultMonitor.DEBUG) log.trace("Skipping redundant {} (already cast in current ballot)", vote);
      responses.put(existing.getCohort(), existing);
      return false;
    }
    
    if (response.getPledge() != Pledge.ACCEPT || hasLapsed(vote)) {
      verdict = Verdict.ABORT;
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
