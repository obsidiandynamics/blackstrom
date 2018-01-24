package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.model.*;

public interface ShardAccumulator {
  void append(Message message);
  
  long getLastOffset();
  
  List<Message> retrieve(long fromOffset);
}
