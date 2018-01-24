package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.BalancedLedgerBroker.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class BalancedLedgerView implements Ledger {
  private final BalancedLedgerBroker broker;
  
  private class Consumer {
    private static final int CONSUME_WAIT_MILLIS = 1;
    
    private final MessageHandler handler;
    private final ConsumerGroup group;
    private final Object handlerId = UUID.randomUUID();
    private final WorkerThread thread;
    private final MessageContext context = new DefaultMessageContext(BalancedLedgerView.this, handlerId);
    private final Accumulator[] accumulators = broker.getAccumulators(); 
    private long[] nextReadOffsets = new long[accumulators.length];
    
    private final List<Message> sink = new ArrayList<>();

    Consumer(MessageHandler handler, ConsumerGroup group) {
      this.handler = handler;
      this.group = group;
      if (group != null) {
        group.join(handler);
      } else {
        for (int shard = 0; shard < accumulators.length; shard++) {
          nextReadOffsets[shard] = accumulators[shard].getNextOffset();
        }
      }
      
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().withDaemon(true).withName(BalancedLedgerView.class.getSimpleName() + "-" + handlerId))
          .onCycle(this::cycle)
          .buildAndStart();
    }
    
    private void cycle(WorkerThread t) throws InterruptedException {
      for (int shard = 0; shard < accumulators.length; shard++) {
        final Accumulator accumulator = accumulators[shard];
        if (group == null || group.isAssignee(shard, handlerId)) {
          final long nextReadOffset;
          if (group != null) {
            nextReadOffset = Math.max(nextReadOffsets[shard], group.getReadOffset(shard));
          } else {
            nextReadOffset = nextReadOffsets[shard];
          }
          
          final int retrieved = accumulator.retrieve(nextReadOffset, sink);
          if (retrieved != 0) {
            nextReadOffsets[shard] = nextReadOffset + retrieved;
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
  }
  
  private final Map<Object, Consumer> consumers = new HashMap<>();
  
  BalancedLedgerView(BalancedLedgerBroker broker) {
    this.broker = broker;
  }

  @Override
  public void attach(MessageHandler handler) {
    final ConsumerGroup group = broker.getOrCreateGroup(handler.getGroupId());
    final Consumer consumer = new Consumer(handler, group);
    consumers.put(consumer.handlerId, consumer);
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    broker.append(message, callback);
  }

  @Override
  public void confirm(Object handlerId, Object messageId) {
    final ConsumerGroup group = consumers.get(handlerId).group;
    if (group != null) {
      final BalancedMessageId balancedId = Cast.from(messageId);
      group.confirm(balancedId.getShard(), balancedId.getOffset());
    }
  }

  @Override
  public synchronized void dispose() {
    final Collection<Consumer> consumers = this.consumers.values();
    consumers.forEach(c -> c.thread.terminate());
    consumers.forEach(c -> c.thread.joinQuietly());
    consumers.stream().filter(c -> c.group != null).forEach(c -> c.group.leave(c.handlerId));
    this.consumers.clear();
  }
}
