package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

/**
 *  A high-performance, lock-free, unbounded MPSC (multi-producer, single-consumer) queue
 *  implementation, adapted from Indigo's scheduler.<p>
 *  
 *  @see <a href="https://github.com/obsidiandynamics/indigo/blob/4b13815d1aefb0e5a5a45ad89444ced9f6584e20/src/main/java/com/obsidiandynamics/indigo/NodeQueueActivation.java">NodeQueueActivation</a>
 */
public final class SingleNodeQueueLedger implements Ledger {
  private volatile MessageHandler[] handlers = new MessageHandler[0];
  
  private final MessageContext context = new DefaultMessageContext(this);
  
  private final WorkerThread thread;
  
  private final AtomicReference<QueueNode> tail = new AtomicReference<>(QueueNode.anchor());

  private AtomicReference<QueueNode> head = tail.get();
  
  public SingleNodeQueueLedger() {
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withDaemon(true)
                     .withName("NodeWorker-" + Long.toHexString(System.identityHashCode(this))))
        .onCycle(this::cycle)
        .build();
    thread.start();
  }
  
  private void cycle(WorkerThread thread) throws InterruptedException {
    final QueueNode n = head.get();
    if (n != null) {
      final Message m = n.m;
      for (MessageHandler handler : handlers) {
        handler.onMessage(context, m);
      }
      head = n;
    } else {
      Thread.sleep(1);
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
    new QueueNode(message).appendTo(tail);
  }

  @Override
  public void dispose() {
    thread.terminate().joinQuietly();
  }
}
