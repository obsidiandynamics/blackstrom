package com.obsidiandynamics.blackstrom.retention;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.flow.*;
import com.obsidiandynamics.flow.Flow;
import com.obsidiandynamics.format.*;
import com.obsidiandynamics.random.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;

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

  private final String groupId;
  
  private final FiringStrategy.Factory firingStrategyFactory;
  
  private final ConcurrentHashMap<Integer, Flow> flows = new ConcurrentHashMap<>();
  
  private final List<Flow> createdFlows = new ArrayList<>();
  
  private final Object terminateLock = new Object();
  
  private boolean terminated;
  
  public ShardedFlow(String groupId) {
    this(groupId, LazyFiringStrategy::new);
  }
  
  public ShardedFlow(String groupId, FiringStrategy.Factory firingStrategyFactory) {
    this.groupId = groupId;
    this.firingStrategyFactory = firingStrategyFactory;
  }

  @Override
  public Confirmation begin(MessageContext context, Message message) {
    final var flow = flows.computeIfAbsent(message.getShard(), shard -> {
      final var randomThreadId = Binary.toHex(Randomness.nextBytes(4));
      final var threadName = Flow.class.getSimpleName() + "-" + randomThreadId + "-shard[" + shard + "]-group[" + groupId + "]";
      final var newFlow = new Flow(firingStrategyFactory, threadName);
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
    final var messageId = message.getMessageId();
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
    final var joiner = Joiner.blank();
    synchronized (terminateLock) {
      joiner.add(createdFlows);
    }
    return joiner.join(timeoutMillis);
  }
}
