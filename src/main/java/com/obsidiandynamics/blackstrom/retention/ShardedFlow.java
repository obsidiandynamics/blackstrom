package com.obsidiandynamics.blackstrom.retention;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.keyed.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

public final class ShardedFlow implements Retention, Joinable {
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
  
  public ShardedFlow() {
    this(LazyFiringStrategy::new);
  }
  
  public ShardedFlow(FiringStrategy.Factory firingStrategyFactory) {
    flows = new Keyed<>(shard -> {
      return new Flow(firingStrategyFactory, Flow.class.getSimpleName() + "-shard-[" + shard + "]");
    });
  }

  @Override
  public Confirmation begin(MessageContext context, Message message) {
    final Flow flow = flows.forKey(message.getShard());
    final MessageId messageId = message.getMessageId();
    return flow.begin(messageId, new ConfirmTask(context, messageId));
  }

  public Joinable terminate() {
    flows.asMap().values().forEach(f -> f.terminate());
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return Joinable.joinAll(timeoutMillis, flows.asMap().values());
  }
}
