package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

public final class FifoShardAssignment extends ShardAssignment {
  private final Set<Object> allHandlers = new CopyOnWriteArraySet<>();

  @Override
  void add(Object handlerId) {
    allHandlers.add(handlerId);
    if (activeHandler == null) {
      activeHandler = handlerId;
    }
  }

  @Override
  void remove(Object handlerId) {
    allHandlers.remove(handlerId);
    if (isAssignee(handlerId)) {
      activeHandler = randomHandler(allHandlers);
    }
  }
}
