package com.obsidiandynamics.blackstrom.monitor.basic;

import java.util.*;

import org.slf4j.*;

import com.obsidiandynamics.blackstrom.model.*;

final class PendingBallot {
  private final Nomination nomination;
  
  private final Map<String, Response> responses;
  
  private Outcome outcome = Outcome.ACCEPT;
  
  PendingBallot(Nomination nomination) {
    this.nomination = nomination;
    responses = new HashMap<>(nomination.getCohorts().length);
  }
  
  Nomination getNomination() {
    return nomination;
  }
  
  Outcome getOutcome() {
    return outcome;
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
      if (BasicMonitor.DEBUG) log.trace("Skipping redundant {} (already cast in current ballot)", vote);
      responses.put(existing.getCohort(), existing);
      return false;
    }
    
    if (response.getPlea() != Plea.ACCEPT) {
      outcome = Outcome.REJECT;
      return true;
    }
    
    return allResponsesPresent();
  }
  
  private boolean allResponsesPresent() {
    return responses.size() == nomination.getCohorts().length;
  }
}
