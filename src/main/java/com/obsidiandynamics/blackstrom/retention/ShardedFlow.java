package com.obsidiandynamics.blackstrom.retention;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;
import com.obsidiandynamics.blackstrom.worker.Terminator;

public final class ShardedFlow implements Retention, Terminable, Joinable {
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
  
  private final List<Flow> createdFlows = new ArrayList<>();
  
  private final Object terminateLock = new Object();
  
  private boolean terminated;
  
  public ShardedFlow() {
    this(LazyFiringStrategy::new);
  }
  
  public ShardedFlow(FiringStrategy.Factory firingStrategyFactory) {
    this.firingStrategyFactory = firingStrategyFactory;
  }

  @Override
  public Confirmation begin(MessageContext context, Message message) {
    final Flow flow = flows.computeIfAbsent(message.getShard(), shard -> {
      final Flow newFlow = new Flow(firingStrategyFactory, Flow.class.getSimpleName() + "-shard-[" + shard + "]");
      synchronized (terminateLock) {
        createdFlows.add(newFlow);
        if (terminated) {
          // the container was already terminated -- terminate the newly created flow; the resulting Confirmation
          // objects won't do anything
          newFlow.terminate();
        }
      }
      return newFlow;
    });
    final MessageId messageId = message.getMessageId();
    return flow.begin(messageId, new ConfirmTask(context, messageId));
  }

  @Override
  public Joinable terminate() {
    synchronized (terminateLock) {
      // as flows are created lazily, it's possible that some flows are created after termination of
      // this container; this flag ensures that new flows are stillborn
      terminated = true;
      Terminator.of(createdFlows).terminate();
    }
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    final Joiner joiner = Joiner.blank();
    synchronized (terminateLock) {
      joiner.add(createdFlows);
    }
    return joiner.join(timeoutMillis);
  }
}
