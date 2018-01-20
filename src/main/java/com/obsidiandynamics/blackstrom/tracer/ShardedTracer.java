package com.obsidiandynamics.blackstrom.tracer;

import java.util.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.keyed.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class ShardedTracer implements Disposable {
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
  
  private final Keyed<Integer, Tracer> tracers = new Keyed<>(shard -> {
    return new Tracer(LazyFiringStrategy::new, Tracer.class.getSimpleName() + "-shard-[" + shard + "]");
  });

  public Action begin(MessageContext context, Message message) {
    final Tracer tracer = tracers.forKey(message.getShard());
    return tracer.begin(new ConfirmTask(context, message.getMessageId()));
  }

  @Override
  public void dispose() {
    final Collection<Tracer> tracers = this.tracers.asMap().values();
    tracers.forEach(t -> t.terminate());
    tracers.forEach(t -> t.joinQuietly());
  }
}
