package com.obsidiandynamics.blackstrom.cohort;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.factor.*;

public interface Cohort extends 
Factor, 
QueryProcessor, 
QueryResponseProcessor,
CommandProcessor, 
CommandResponseProcessor,
NoticeProcessor, 
ProposalProcessor, 
VoteProcessor, 
OutcomeProcessor {
  
  interface Base extends 
  Cohort, 
  Initable.Nop, 
  Disposable.Nop, 
  QueryProcessor.BeginAndConfirm, 
  QueryResponseProcessor.BeginAndConfirm,
  CommandProcessor.BeginAndConfirm, 
  CommandResponseProcessor.BeginAndConfirm,
  NoticeProcessor.BeginAndConfirm,
  ProposalProcessor.BeginAndConfirm, 
  VoteProcessor.BeginAndConfirm, 
  OutcomeProcessor.BeginAndConfirm {}
}
