package com.obsidiandynamics.blackstrom.retention;

import java.util.*;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.keyed.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

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
  
  private final Keyed<Integer, Flow> flows;
  
  private final List<Flow> createdFlows = new ArrayList<>();
  
  private final Object terminateLock = new Object();
  
  private boolean terminated;
  
  public ShardedFlow() {
    this(LazyFiringStrategy::new);
  }
  
  public ShardedFlow(FiringStrategy.Factory firingStrategyFactory) {
    flows = new Keyed<>(shard -> {
      synchronized (terminateLock) {
        final Flow flow = new Flow(firingStrategyFactory, Flow.class.getSimpleName() + "-shard-[" + shard + "]");
        createdFlows.add(flow);
        if (terminated) {
          // the container was already terminated -- terminate the newly created flow; the resulting Confirmation
          // objects won't do anything
          flow.terminate();
        }
        return flow;
      }
    });
  }

  @Override
  public Confirmation begin(MessageContext context, Message message) {
    final Flow flow = flows.forKey(message.getShard());
    final MessageId messageId = message.getMessageId();
    return flow.begin(messageId, new ConfirmTask(context, messageId));
  }

  @Override
  public Joinable terminate() {
    synchronized (terminateLock) {
      // as flows are created lazily, it's possible that some flows are created after termination of
      // this container; this flag ensures that new flows are stillborn
      terminated = true;
      createdFlows.forEach(f -> f.terminate());
    }
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    final List<Flow> createdFlowsCopy; 
    synchronized (terminateLock) {
      createdFlowsCopy = new ArrayList<>(createdFlows);
    }
    return Joinable.joinAll(timeoutMillis, createdFlowsCopy);
  }
}
