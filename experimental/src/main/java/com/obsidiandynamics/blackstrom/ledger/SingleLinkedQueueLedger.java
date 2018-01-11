package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

/**
 *  In-memory ledger implementation comprising a single blocking queue with a single-threaded
 *  dispatcher for all subscribers hanging off that queue.
 */
public final class SingleLinkedQueueLedger implements Ledger {
  private volatile MessageHandler[] handlers = new MessageHandler[0];
  
  private final MessageContext context = new DefaultMessageContext(this, null);
  
  private final BlockingQueue<Message> queue;
  
  private final WorkerThread thread;
  
  public SingleLinkedQueueLedger() {
    queue = new LinkedBlockingQueue<>();
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withDaemon(true)
                     .withName(SingleLinkedQueueLedger.class.getSimpleName() + "-" + Integer.toHexString(System.identityHashCode(this))))
        .onCycle(this::cycle)
        .build();
    thread.start();
  }
  
  private void cycle(WorkerThread thread) throws InterruptedException {
    final Message message = queue.take();
    for (MessageHandler handler : handlers) {
      handler.onMessage(context, message);
    }
  }
  
  @Override
  public void attach(MessageHandler handler) {
    final List<MessageHandler> handlersList = new ArrayList<>(Arrays.asList(handlers));
    handlersList.add(handler);
    handlers = handlersList.toArray(new MessageHandler[handlersList.size()]);
  }

  @Override
  public void append(Message message) throws Exception {
    queue.put(message);
  }
  
  @Override
  public void confirm(Object handlerId, Object messageId) {}

  @Override
  public void dispose() {
    thread.terminate().joinQuietly();
  }
}
