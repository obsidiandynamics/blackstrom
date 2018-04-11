package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.BalancedLedgerHub.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;

public final class BalancedLedgerView implements Ledger {
  private final BalancedLedgerHub hub;
  
  private final List<ShardedFlow> flows = new ArrayList<>(); 
  
  private final Accumulator[] accumulators;
  
  private class Consumer implements Terminable {
    private static final int CONSUME_WAIT_MILLIS = 1;
    
    private final MessageHandler handler;
    private final ConsumerGroup group;
    private final Object handlerId = UUID.randomUUID();
    private final WorkerThread thread;
    private final MessageContext context;
    private long[] nextReadOffsets = new long[accumulators.length];
    
    private final List<Message> sink = new ArrayList<>();

    Consumer(MessageHandler handler, ConsumerGroup group) {
      this.handler = handler;
      this.group = group;
      final Retention retention;
      if (group != null) {
        group.join(handlerId);
        final ShardedFlow flow = new ShardedFlow();
        flows.add(flow);
        retention = flow;
      } else {
        Arrays.setAll(nextReadOffsets, shard -> accumulators[shard].getNextOffset());
        retention = NopRetention.getInstance();
      } 
      context = new DefaultMessageContext(BalancedLedgerView.this, handlerId, retention);
      
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().daemon().withName(BalancedLedgerView.class, handlerId))
          .onCycle(this::cycle)
          .build();
    }
    
    void start() {
      thread.start();
    }
    
    private void cycle(WorkerThread t) throws InterruptedException {
      for (int shard = 0; shard < accumulators.length; shard++) {
        if (group == null || group.isAssignee(shard, handlerId)) {
          final Accumulator accumulator = accumulators[shard];
          final long nextReadOffset;
          if (group != null) {
            nextReadOffset = Math.max(nextReadOffsets[shard], group.getReadOffset(shard));
          } else {
            nextReadOffset = nextReadOffsets[shard];
          }
          
          final int retrieved = accumulator.retrieve(nextReadOffset, sink);
          if (retrieved != 0) {
            nextReadOffsets[shard] = ((DefaultMessageId) sink.get(sink.size() - 1).getMessageId()).getOffset() + 1;
          }
        }
      }
      
      if (! sink.isEmpty()) {
        for (Message message : sink) {
          handler.onMessage(context, message);
        }
        sink.clear();
      } else {
        Thread.sleep(CONSUME_WAIT_MILLIS);
      }
    }

    @Override
    public Joinable terminate() {
      return thread.terminate();
    }
  }
  
  private final Map<Object, Consumer> consumers = new HashMap<>();
  
  private boolean detached;
  
  BalancedLedgerView(BalancedLedgerHub hub) {
    this.hub = hub;
    accumulators = hub.getAccumulators(); 
  }
  
  public BalancedLedgerHub getHub() {
    return hub;
  }
  
  public void detach() {
    detached = true;
  }

  @Override
  public void attach(MessageHandler handler) {
    final ConsumerGroup group = handler.getGroupId() != null ? hub.getOrCreateGroup(handler.getGroupId()) : null;
    final Consumer consumer = new Consumer(handler, group);
    consumers.put(consumer.handlerId, consumer);
    consumer.start();
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    hub.append(message, callback);
    callback.onAppend(message.getMessageId(), null);
  }

  @Override
  public void confirm(Object handlerId, MessageId messageId) {
    final Consumer consumer = consumers.get(handlerId);
    final ConsumerGroup group = consumer.group;
    final DefaultMessageId defaultMessageId = (DefaultMessageId) messageId;
    group.confirm(defaultMessageId.getShard(), defaultMessageId.getOffset());
  }

  @Override
  public void dispose() {
    hub.removeView(this);
    final Collection<Consumer> consumers = this.consumers.values();
    
    Terminator.blank()
    .add(consumers)
    .add(flows)
    .terminate()
    .joinSilently();

    consumers.stream().filter(c -> c.group != null).forEach(c -> c.group.leave(c.handlerId));
    
    if (! detached) {
      hub.dispose();
    }
  }
}
