package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

/**
 *  Simple, in-memory ledger implementation comprising multiple discrete blocking queues - one for each
 *  subscriber.
 */
public final class MultiLinkedQueueLedger implements Ledger {
  private final List<BlockingQueue<Message>> queues = new ArrayList<>();
  private final List<WorkerThread> threads = new CopyOnWriteArrayList<>();
  private final Object lock = new Object();
  
  private final MessageContext context = new DefaultMessageContext(this);
  
  @Override
  public void attach(MessageHandler handler) {
    final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    final WorkerThread thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName("LinkedQueueWorker-" + Long.toHexString(System.identityHashCode(handler)))
                     .withDaemon(true))
        .withWorker(t -> consumeAndDispatch(queue, t, handler))
        .build();
    threads.add(thread);
    thread.start();
    synchronized (lock) {
      queues.add(queue);
    }
  }
  
  private void consumeAndDispatch(BlockingQueue<Message> queue, WorkerThread thread, MessageHandler handler) throws InterruptedException {
    final Message m = queue.take();
    handler.onMessage(context, m);
  }
  
  @Override
  public void append(Message message) throws InterruptedException {
    synchronized (lock) {
      for (int i = queues.size(); --i >= 0; ) {
        queues.get(i).put(message);
      }
    }
  }
  
  @Override
  public void dispose() {
    threads.forEach(t -> t.terminate());
    threads.forEach(t -> t.joinQuietly());
  }
}
