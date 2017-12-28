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
public final class SingleQueueLedger implements Ledger {
  private final List<MessageHandler> handlers = new CopyOnWriteArrayList<>();
  
  private final VotingContext context = new DefaultVotingContext(this);
  
  private final BlockingQueue<Message> queue;
  
  private final WorkerThread thread;
  
  public SingleQueueLedger() {
    queue = new LinkedBlockingQueue<>();
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withDaemon(true)
                     .withName("MessageWorker-" + Long.toHexString(System.identityHashCode(this))))
        .withWorker(this::cycle)
        .build();
    thread.start();
  }
  
  private void cycle(WorkerThread thread) throws InterruptedException {
    final Message message = queue.take();
    for (int i = handlers.size(); --i >= 0; ) {
      handlers.get(i).onMessage(context, message);
    }
  }
  
  @Override
  public void attach(MessageHandler handler) {
    handlers.add(handler);
  }

  @Override
  public void append(Message message) throws Exception {
    queue.put(message);
  }

  @Override
  public void dispose() {
    thread.terminate();
    thread.joinQuietly();
  }
}
