package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

public abstract class ShardAssignment {
  protected volatile Object activeHandler;

  abstract void add(Object handlerId);

  abstract void remove(Object handlerId);
  
  final boolean isAssignee(Object handlerId) {
    return handlerId.equals(activeHandler);
  }

  protected static Object randomHandler(Collection<Object> allHandlers) {
    if (! allHandlers.isEmpty()) {
      final List<Object> handlers = new ArrayList<>(allHandlers);
      return handlers.get((int) (Math.random() * handlers.size()));
    } else {
      return null;
    }
  }
  
  @FunctionalInterface
  public interface Factory {
    ShardAssignment create();
  }
}
