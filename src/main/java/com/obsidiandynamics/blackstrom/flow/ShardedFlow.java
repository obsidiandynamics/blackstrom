package com.obsidiandynamics.blackstrom.flow;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.keyed.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class ShardedFlow implements Disposable {
  private static class ConfirmTask implements Runnable {
    private final MessageContext context;
    private final Object messageId;
    
    ConfirmTask(MessageContext context, Object messageId) {
      this.context = context;
      this.messageId = messageId;
    }

    @Override
    public void run() {
      context.confirm(messageId);
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

  public Confirmation begin(MessageContext context, Message message) {
    final Flow flow = flows.forKey(message.getShard());
    return flow.begin(new ConfirmTask(context, message.getMessageId()));
  }

  @Override
  public void dispose() {
    final Collection<Flow> flows = this.flows.asMap().values();
    flows.forEach(t -> t.terminate());
    flows.forEach(t -> t.joinQuietly());
  }
}
