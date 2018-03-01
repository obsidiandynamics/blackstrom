package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class StackLedger implements Ledger {
  private final List<MessageHandler> handlers = new ArrayList<>();
  
  private final LinkedList<Message> queue = new LinkedList<>();
  
  private final Object lock = new Object();
  
  private final MessageContext context = new DefaultMessageContext(this, null);
  
  /** Tracks presence of group members. */
  private final Set<String> groups = new HashSet<>();
  
  private boolean delivering;

  @Override
  public void attach(MessageHandler handler) {
    if (handler.getGroupId() != null && ! groups.add(handler.getGroupId())) return;
    
    handlers.add(handler);
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
          for (int i = handlers.size(); --i >= 0;) {
            handlers.get(i).onMessage(context, head);
          }
        } while (! queue.isEmpty());
        delivering = false;
      }
    }
  }

  @Override
  public void confirm(Object handlerId, MessageId messageId) {}
}
