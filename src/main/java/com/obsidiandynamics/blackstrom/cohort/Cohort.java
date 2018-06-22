package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.factor.*;

public interface Cohort extends 
Factor, QueryProcessor, CommandProcessor, NoticeProcessor, ProposalProcessor, VoteProcessor, OutcomeProcessor {
  
  interface Choreography extends Cohort, QueryProcessor.Nop, CommandProcessor.Nop, NoticeProcessor.Nop {}
}
