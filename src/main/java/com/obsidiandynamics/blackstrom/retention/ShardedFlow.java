package com.obsidiandynamics.blackstrom.retention;

import static com.obsidiandynamics.func.Functions.*;

import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.flow.*;
import com.obsidiandynamics.flow.Flow;

/**
 *  A sharding of a {@link ThreadlessFlow}.
 */
public final class ShardedFlow implements Retention {
  private static class ConfirmTask implements Runnable {
    private final MessageContext context;
    private final MessageId messageId;
    
    ConfirmTask(MessageContext context, MessageId messageId) {
      this.context = context;
      this.messageId = messageId;
    }

    @Override
    public void run() {
      context.getLedger().confirm(context.getHandlerId(), messageId);
    }
  }

  private final FiringStrategy.Factory firingStrategyFactory;
  
  private final ConcurrentHashMap<Integer, Flow> flows = new ConcurrentHashMap<>();
  
  public ShardedFlow() {
    this(LazyFiringStrategy::new);
  }
  
  public ShardedFlow(FiringStrategy.Factory firingStrategyFactory) {
    this.firingStrategyFactory = mustExist(firingStrategyFactory, "Firing strategy factory cannot be null");
  }

  @Override
  public Confirmation begin(MessageContext context, Message message) {
    mustExist(context, "Context cannot be null");
    mustExist(message, "Message cannot be null");
    final var flow = flows.computeIfAbsent(message.getShard(), shard -> new ThreadlessFlow(firingStrategyFactory));
    final var messageId = message.getMessageId();
    return flow.begin(messageId, new ConfirmTask(context, messageId));
  }
}
