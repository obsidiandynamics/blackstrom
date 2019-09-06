package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.BalancedLedgerHub.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;
import com.obsidiandynamics.zerolog.*;
import com.obsidiandynamics.zerolog.util.*;

public final class BalancedLedgerView implements Ledger {
  private Zlg zlg = Zlg.forDeclaringClass().get();
  
  private final BalancedLedgerHub hub;
  
  private final Accumulator[] accumulators;
  
  private final class Consumer implements Terminable {
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
        retention = flow;
      } else {
        Arrays.setAll(nextReadOffsets, shard -> accumulators[shard].getNextOffset());
        retention = NopRetention.getInstance();
      } 
      context = new DefaultMessageContext(BalancedLedgerView.this, handlerId, retention);
      
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().daemon().withName(BalancedLedgerView.class, handlerId))
          .onUncaughtException(new ZlgWorkerExceptionHandler(zlg))
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
            final long localNextReadOffset = nextReadOffsets[shard];
            final long groupNextReadOffset = group.getReadOffset(shard);
            if (localNextReadOffset < groupNextReadOffset) {
              zlg.d("Read offset advanced for group %s: local: %,d, group: %,d", 
                    z -> z.arg(group.getGroupId()).arg(localNextReadOffset).arg(groupNextReadOffset));
            }
            nextReadOffset = Math.max(localNextReadOffset, groupNextReadOffset);
          } else {
            nextReadOffset = nextReadOffsets[shard];
          }
          
          final int retrieved = accumulator.retrieve(nextReadOffset, sink);
          if (retrieved != 0) {
            final long offsetOfLastItem = ((DefaultMessageId) sink.get(sink.size() - 1).getMessageId()).getOffset();
            final long newReadOffset = offsetOfLastItem + 1;
            if (newReadOffset - nextReadOffset != retrieved) {
              // detect buffer discontinuities, which can occur if the reader can't keep up with the writer
              final long offsetOfFirstItem = ((DefaultMessageId) sink.get(sink.size() - retrieved).getMessageId()).getOffset();
              zlg.w("Buffer overflow: next expected read: %,d, but read %,d item(s) between offsets %,d and %,d", 
                    z -> z.arg(nextReadOffset).arg(retrieved).arg(offsetOfFirstItem).arg(offsetOfLastItem));
            }
            nextReadOffsets[shard] = newReadOffset;
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
  
  public BalancedLedgerView withZlg(Zlg zlg) {
    this.zlg = zlg;
    return this;
  }

  @Override
  public Object attach(MessageHandler handler) {
    final ConsumerGroup group = handler.getGroupId() != null ? hub.getOrCreateGroup(handler.getGroupId()) : null;
    final Consumer consumer = new Consumer(handler, group);
    consumers.put(consumer.handlerId, consumer);
    consumer.start();
    return consumer.handlerId;
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
    mustExist(group, illegalState("Cannot confirm in an ungrouped context"));
    final DefaultMessageId defaultMessageId = (DefaultMessageId) messageId;
    group.confirm(defaultMessageId.getShard(), defaultMessageId.getOffset() + 1);
  }

  @Override
  public boolean isAssigned(Object handlerId, int shard) {
    final Consumer consumer = consumers.get(handlerId);
    final ConsumerGroup group = consumer.group;
    return group == null || group.isAssignee(shard, handlerId);
  }

  @Override
  public void dispose() {
    hub.removeView(this);
    final Collection<Consumer> consumers = this.consumers.values();
    
    Terminator.blank()
    .add(consumers)
    .terminate()
    .joinSilently();

    consumers.stream().filter(c -> c.group != null).forEach(c -> c.group.leave(c.handlerId));
    
    if (! detached) {
      hub.dispose();
    }
  }
}
