package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface Accumulator extends Disposable.Nop {
  void append(Message message);
  
  long getNextOffset();
  
  int retrieve(long fromOffset, List<Message> sink);
  
  @FunctionalInterface
  interface Factory {
    Accumulator create(int shard);
  }
}
