package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class BalancedLedgerBroker implements Disposable {
  private final Accumulator[] accumulators;

  static class Assignment {
    private volatile Object activeHandler;
    private final Set<Object> allHandlers = new CopyOnWriteArraySet<>();

    void add(Object handlerId) {
      allHandlers.add(handlerId);
      if (activeHandler == null) {
        activeHandler = handlerId;
      }
    }

    void remove(Object handlerId) {
      allHandlers.remove(handlerId);
      if (handlerId.equals(activeHandler)) {
        activeHandler = randomHandler();
      }
    }

    private Object randomHandler() {
      final List<Object> handlers = new ArrayList<>(allHandlers);
      return handlers.get((int) (Math.random() * handlers.size()));
    }
  }

  class ConsumerGroup {
    private final String groupId;
    private final AtomicLong[] offsets = new AtomicLong[accumulators.length];
    private final Assignment[] assignments = new Assignment[accumulators.length];

    ConsumerGroup(String groupId) {
      this.groupId = groupId;
      for (int i = 0; i < accumulators.length; i++) {
        offsets[i] = new AtomicLong();
        assignments[i] = new Assignment();
      }
    }

    void join(Object handlerId) {
      Arrays.stream(assignments).forEach(a -> a.add(handlerId));
    }

    void leave(Object handlerId) {
      Arrays.stream(assignments).forEach(a -> a.remove(handlerId));
    }
    
    long getReadOffset(int shard) {
      return offsets[shard].get();
    }

    boolean isAssignee(int shard, Object handlerId) {
      return assignments[shard].activeHandler.equals(handlerId);
    }

    void confirm(int shard, long offset) {
      offsets[shard].set(offset);
    }
  }

  private final Map<String, ConsumerGroup> groups = new HashMap<>();
  
  private final Set<BalancedLedgerView> views = new HashSet<>();

  private final Object lock = new Object();

  public BalancedLedgerBroker(int shards) {
    accumulators = new Accumulator[shards];
  }

  public BalancedLedgerView createView() {
    final BalancedLedgerView view = new BalancedLedgerView(this);
    views.add(view);
    return view;
  }

  void append(Message message, AppendCallback callback) {
    final int shard = Hash.getShard(message, accumulators.length);
    final Accumulator accumulator = accumulators[shard];
    accumulator.append(message);
  }

  ConsumerGroup getOrCreateGroup(String groupId) {
    synchronized (lock) {
      final ConsumerGroup existing = groups.get(groupId);
      if (existing != null) {
        return existing;
      } else {
        final ConsumerGroup created = new ConsumerGroup(groupId);
        groups.put(groupId, created);
        return created;
      }
    }
  }

  Accumulator[] getAccumulators() {
    return accumulators;
  }

  @Override
  public void dispose() {
    Arrays.stream(accumulators).forEach(a -> a.dispose());
    views.forEach(v -> v.dispose());
  }
}
