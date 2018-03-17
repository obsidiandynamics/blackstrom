package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class BalancedLedgerHub implements Disposable {
  private final ShardAssignment.Factory shardAssignmentFactory;
  
  private final Accumulator[] accumulators;

  class ConsumerGroup {
    private final AtomicLong[] offsets = new AtomicLong[accumulators.length];
    private final ShardAssignment[] assignments = new ShardAssignment[accumulators.length];

    ConsumerGroup() {
      Arrays.setAll(offsets, i-> new AtomicLong());
      Arrays.setAll(assignments, i -> shardAssignmentFactory.create());
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
      return assignments[shard].isAssignee(handlerId);
    }

    void confirm(int shard, long offset) {
      offsets[shard].set(offset);
    }
  }

  private final Map<String, ConsumerGroup> groups = new HashMap<>();
  
  private final Set<BalancedLedgerView> views = new CopyOnWriteArraySet<>();

  private final Object lock = new Object();
  
  public BalancedLedgerHub(int shards, ShardAssignment.Factory shardAssignmentFactory, Accumulator.Factory accumulatorFactory) {
    this.shardAssignmentFactory = shardAssignmentFactory;
    accumulators = new Accumulator[shards];
    Arrays.setAll(accumulators, accumulatorFactory::create);
  }

  public BalancedLedgerView connect() {
    final BalancedLedgerView view = new BalancedLedgerView(this);
    views.add(view);
    return view;
  }
  
  public BalancedLedgerView connectDetached() {
    final BalancedLedgerView view = connect();
    view.detach();
    return view;
  }
  
  public int getShards() {
    return accumulators.length;
  }
  
  public Set<BalancedLedgerView> getViews() {
    return Collections.unmodifiableSet(views);
  }
  
  void removeView(BalancedLedgerView view) {
    views.remove(view);
  }

  void append(Message message, AppendCallback callback) {
    final int shard = Hash.getShard(message, accumulators.length);
    message.setShard(shard);
    final Accumulator accumulator = accumulators[shard];
    accumulator.append(message);
  }

  ConsumerGroup getOrCreateGroup(String groupId) {
    synchronized (lock) {
      final ConsumerGroup existing = groups.get(groupId);
      if (existing != null) {
        return existing;
      } else {
        final ConsumerGroup created = new ConsumerGroup();
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
    views.clear();
  }
}
