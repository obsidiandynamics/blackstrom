package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.func.*;

public final class StackLedger implements Ledger {
  private final Set<UUID> subscribedHandlerIds = new CopyOnWriteArraySet<>();
  
  private final LinkedList<Message> queue = new LinkedList<>();
  
  private final Object lock = new Object();
  
  /** Tracks presence of group members. */
  private final Set<String> groups = new CopyOnWriteArraySet<>();
  
  private boolean delivering;
  
  private static final class ContextualHandler {
    final MessageHandler handler;
    
    final MessageContext context;

    ContextualHandler(MessageHandler handler, MessageContext context) {
      this.handler = handler;
      this.context = context;
    }
  }
  
  private volatile ContextualHandler[] contextualHandlers = new ContextualHandler[0];

  @Override
  public void attach(MessageHandler handler) {
    if (handler.getGroupId() != null && ! groups.add(handler.getGroupId())) return;
    
    final UUID handlerId;
    if (handler.getGroupId() != null) {
      handlerId = UUID.randomUUID();
      subscribedHandlerIds.add(handlerId);
    } else {
      handlerId = null;
    }
    
    final MessageContext context = new DefaultMessageContext(this, handlerId, NopRetention.getInstance());
    contextualHandlers = ArrayCopy.append(contextualHandlers, new ContextualHandler(handler, context));
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    synchronized (lock) {
      queue.addLast(message);
      callback.onAppend(message.getMessageId(), null);
      if (! delivering) {
        delivering = true;
        do {
          final Message head = queue.removeFirst();
          for (ContextualHandler contextualHandler : contextualHandlers) {
            contextualHandler.handler.onMessage(contextualHandler.context, head);
          }
        } while (! queue.isEmpty());
        delivering = false;
      }
    }
  }

  @Override
  public boolean isAssigned(Object handlerId, int shard) {
    return handlerId == null || subscribedHandlerIds.contains(handlerId);
  }
}
