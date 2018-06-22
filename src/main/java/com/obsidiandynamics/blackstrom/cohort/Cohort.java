package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.factor.*;

public interface Cohort 
extends Factor, QueryProcessor, CommandProcessor, NoticeProcessor, ProposalProcessor, VoteProcessor, OutcomeProcessor {
  
  interface Base 
  extends Cohort, QueryProcessor.Nop, CommandProcessor.Nop, NoticeProcessor.Nop,
  ProposalProcessor.Nop, VoteProcessor.Nop, OutcomeProcessor.Nop {}
}
