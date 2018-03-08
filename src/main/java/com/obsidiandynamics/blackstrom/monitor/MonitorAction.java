package com.obsidiandynamics.blackstrom.monitor;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface MonitorAction {
  void appendVote(Vote vote, AppendCallback callback);
  
  void appendOutcome(Outcome outcome, AppendCallback callback);
}
